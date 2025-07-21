import asyncio
from datetime import datetime, timedelta, timezone
import os
import httpx
from motor.motor_asyncio import AsyncIOMotorClient
from math import floor
import json
import logging
import sys
from prometheus_client import Counter, Gauge, start_http_server

# Определение метрик с ключом namespace вместо subdomain
deployments_processed = Counter('billing_deployments_processed_total', 'Total number of deployments processed')
errors_total = Counter('billing_errors_total', 'Total number of errors in billing loop', ['error_type'])
user_balance = Gauge('billing_user_balance', 'Current balance of users', ['namespace'])
cost_total = Counter('billing_cost_total', 'Total cost billed', ['namespace'])
partial_cost_total = Counter('billing_partial_cost_total', 'Total partial cost billed due to insufficient funds', ['namespace'])
no_funds_total = Counter('billing_no_funds_total', 'Total number of deployments stopped due to insufficient funds', ['namespace'])

# СЧЕТЧИК НА КОЛИЧЕСТВО ОБРАБОТАННЫХ РАЗВЕРТЫВАНИЙ
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME")
BILLING_INTERVAL = timedelta(minutes=60)
METRICS_SAVE_INTERVAL = 300

if DB_NAME is None:
    raise ValueError("DB_NAME environment variable is not set.")

# Загрузка тарифов
with open('tarifs.json', 'r') as f:
    tarifs = json.load(f)

with open('tarifs_db.json', 'r') as f:
    tarifs_db = json.load(f)

# Подключение к MongoDB
client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]
users_collection = db["users"]
deployments_collection = db["deployments"]
metrics_collection = db["metrics"]

K8S_MANAGER = "http://k8s-manager-service"
    
# Настройка логгера
logger = logging.getLogger("billing")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('{"timestamp":"%(asctime)s", "level":"%(levelname)s", "message":%(message)s}')
handler.setFormatter(formatter)
logger.addHandler(handler)

async def load_metrics_state():
    """Загрузка сохраненных значений метрик из MongoDB при старте."""
    async for metric in metrics_collection.find():
        metric_name = metric["name"]
        value = metric["value"]
        labels = metric.get("labels", {})

        # Ensure labels is a dictionary
        if not isinstance(labels, dict):
            logger.warning(f"Invalid labels format for metric {metric_name}: {labels}. Expected a dictionary, got {type(labels)}. Skipping.")
            continue

        try:
            if metric_name == "billing_deployments_processed_total":
                deployments_processed.inc(value)
            elif metric_name == "billing_errors_total":
                errors_total.labels(**labels).inc(value)
            elif metric_name == "billing_cost_total":
                cost_total.labels(**labels).inc(value)
            elif metric_name == "billing_partial_cost_total":
                partial_cost_total.labels(**labels).inc(value)
            elif metric_name == "billing_no_funds_total":
                no_funds_total.labels(**labels).inc(value)
        except Exception as e:
            logger.error(f"Failed to load metric {metric_name} with labels {labels}: {str(e)}")
            errors_total.labels(error_type="metric_load_failed").inc()

async def save_metrics_state():
    """Сохранение текущих значений метрик в MongoDB."""
    # Сохранение deployments_processed
    await metrics_collection.update_one(
        {"name": "billing_deployments_processed_total"},
        {"$set": {"value": deployments_processed._value.get()}},
        upsert=True
    )

    # Сохранение errors_total
    for labels, metric in errors_total._metrics.items():
        await metrics_collection.update_one(
            {"name": "billing_errors_total", "labels": labels},
            {"$set": {"value": metric._value.get()}},
            upsert=True
        )

    # Сохранение cost_total
    for labels, metric in cost_total._metrics.items():
        await metrics_collection.update_one(
            {"name": "billing_cost_total", "labels": labels},
            {"$set": {"value": metric._value.get()}},
            upsert=True
        )

    # Сохранение partial_cost_total
    for labels, metric in partial_cost_total._metrics.items():
        await metrics_collection.update_one(
            {"name": "billing_partial_cost_total", "labels": labels},
            {"$set": {"value": metric._value.get()}},
            upsert=True
        )

    # Сохранение no_funds_total
    for labels, metric in no_funds_total._metrics.items():
        await metrics_collection.update_one(
            {"name": "billing_no_funds_total", "labels": labels},
            {"$set": {"value": metric._value.get()}},
            upsert=True
        )

    logger.info('"Metrics saved to MongoDB"')

async def metrics_save_loop():
    """Фоновая задача для периодического сохранения метрик."""
    while True:
        await save_metrics_state()
        await asyncio.sleep(METRICS_SAVE_INTERVAL)

async def billing_loop():
    logger.info('Billing loop is starting...')
    start_http_server(8000)
    logger.info('HTTP Server Started')
    await load_metrics_state()
    logger.info('Metrics has been loaded')

    asyncio.create_task(metrics_save_loop())
    logger.info('Metrics save task created')

    while True:
        now = datetime.now(timezone.utc)
        cursor = deployments_collection.find({"status": "running"})
        async for deployment in cursor:
            id = deployment.get("user_id")
            lastTimePay = deployment.get("lastTimePay")

            lastTimePay = lastTimePay.replace(tzinfo=timezone.utc)
            delta = now - lastTimePay

            intervals_passed = floor(delta.total_seconds() / BILLING_INTERVAL.total_seconds())
            if intervals_passed <= 0:
                continue

            # Увеличиваем счетчик обработанных развертываний
            deployments_processed.inc()

            user = await users_collection.find_one({"_id": id})  # Замена subdomain на namespace
            if not user:
                logger.warning(f'"Пользователь с _id {str(id)} не найден"')
                errors_total.labels(error_type="user_not_found").inc()
                continue

            if deployment.get("type", "microservice") == "microservice":
                BILLING_COST = tarifs[deployment.get("tarif", "standart")]['hourPrice']
            else:
                BILLING_COST = tarifs_db[deployment.get("tarif", "standart")]['hourPrice']

            total_cost = intervals_passed * BILLING_COST
            user_balance_value = user["balance"]

            # Обновляем gauge с текущим балансом пользователя
            user_balance.labels(namespace=user['namespace']).set(user_balance_value)

            if user_balance_value >= total_cost:
                await users_collection.update_one(
                    {"_id": user["_id"]},
                    {"$inc": {"balance": -total_cost}}
                )
                await deployments_collection.update_one(
                    {"_id": deployment["_id"]},
                    {"$set": {"lastTimePay": lastTimePay + BILLING_INTERVAL * intervals_passed}}
                )
                # Регистрируем полное списание
                cost_total.labels(namespace=user['namespace']).inc(total_cost)
                logger.info(f'"[OK] Списано {total_cost} за {intervals_passed} ч. у пользователя {namespace}"')
            else:
                max_intervals = floor(user_balance_value / BILLING_COST)
                if max_intervals > 0:
                    partial_cost = max_intervals * BILLING_COST
                    await users_collection.update_one(
                        {"_id": user["_id"]},
                        {"$inc": {"balance": -partial_cost}}
                    )
                    await deployments_collection.update_one(
                        {"_id": deployment["_id"]},
                        {"$set": {"lastTimePay": lastTimePay + BILLING_INTERVAL * max_intervals}}
                    )
                    # Регистрируем частичное списание
                    partial_cost_total.labels(namespace=user['namespace']).inc(partial_cost)
                    logger.info(f'"[PARTIAL] Списано {partial_cost} за {max_intervals} ч. у пользователя {namespace}"')
                else:
                    await deployments_collection.update_one(
                        {"_id": deployment["_id"]},
                        {"$set": {
                            "status": "waitToPay",
                            "uptime_start": datetime(1970, 1, 1, tzinfo=timezone.utc),
                        }}
                    )
                    # Регистрируем остановку из-за недостатка средств
                    no_funds_total.labels(namespace=user['namespace']).inc()

                    k8s_manager_url = K8S_MANAGER + "/api/v1/set/scale"
                    data = {
                        "namespace": user['namespace'], 
                        "deployment_name": deployment.get("deployment_name"),
                        "replicas": 0,
                    }
                    try:
                        async with httpx.AsyncClient() as client:
                            response = await client.post(k8s_manager_url, json=data)
                            response.raise_for_status()
                            data_response = response.json()
                    except Exception as e:
                        logger.error(f'"Failed to contact k8s-manager: {str(e)}"')
                        errors_total.labels(error_type="k8s_contact_failed").inc()

                    logger.warning(f'"[NOFUNDS] Недостаточно средств у пользователя {str(id)}"')

        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(billing_loop())