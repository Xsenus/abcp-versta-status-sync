import requests
import json
import sys
import logging
from datetime import datetime, timedelta
import calendar
from dotenv import load_dotenv
import os
import time

# === Логирование ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

load_dotenv()

# === Настройки ===
ABCP_BASE_URL = os.getenv("ABCP_BASE_URL") or ""
ABCP_ORDER_UPDATE_URL = os.getenv("ABCP_ORDER_UPDATE_URL") or ""
VERSTA_API_URL = os.getenv("VERSTA_API_URL") or ""
ABCP_USER = os.getenv("ABCP_USER")
ABCP_PSW = os.getenv("ABCP_PSW")
VERSTA_TOKEN = os.getenv("VERSTA_TOKEN")

HEADERS_VERSTA = {
    "Authorization": VERSTA_TOKEN,
    "Accept": "application/json"
}

VERSTA_TO_ABCP = {
    # === Доставляется ===
    (100, 6):  {"id": 405204, "name": "Доставляется"},
    (100, 7):  {"id": 405204, "name": "Доставляется"},
    (100, 8):  {"id": 405204, "name": "Доставляется"},
    (100, 9):  {"id": 405204, "name": "Доставляется"},
    (100, 10): {"id": 405204, "name": "Доставляется"},
    (100, 11): {"id": 405204, "name": "Доставляется"},
    (100, None): {"id": 405204, "name": "Доставляется"},

    # === Доставлен ===
    (200, 12): {"id": 405205, "name": "Доставлен"},
    (200, None): {"id": 405205, "name": "Доставлен"},

    # === Получен ===
    (700, 90): {"id": 385106, "name": "Получен"},
    (700, None): {"id": 385106, "name": "Получен"},

    # === Возвращается ===
    (800, 95): {"id": 385107, "name": "Возвращается"},
    (800, None): {"id": 385107, "name": "Возвращается"},

    # === Проблема доставки ===
    ("ProblemDetected", None): {"id": 409382, "name": "Проблема доставки"},

    # === Отменен поставщиком ===
    ("Returning", None): {"id": 384501, "name": "Отменен поставщиком"},

    # === Отменен по неоплате ===
    (600, None): {"id": 404714, "name": "Отменен по неоплате"},
    ("Cancelled", None): {"id": 404714, "name": "Отменен по неоплате"},

    # === Вернулся в ТК ===
    (60, None): {"id": 418655, "name": "Вернулся в ТК"},
    ("NotDelivered", None): {"id": 418655, "name": "Вернулся в ТК"},

    # === Возврат принят ===
    ("ReturnAccepted", None): {"id": 418656, "name": "Возврат принят"},

    # === Новый заказ ===
    (0, None): {"id": 409381, "name": "Новый"},

    # === Готов к выдаче (предположение для 300) ===
    (300, None): {"id": 405204, "name": "Доставляется"},
}

# def get_date_range():
#     today = datetime.today()
#     prev = today.replace(day=1) - timedelta(days=1)
#     next_month = today.replace(day=28) + timedelta(days=4)
#     next_last = next_month.replace(day=calendar.monthrange(next_month.year, next_month.month)[1])
#     return prev.replace(day=1).strftime("%Y-%m-%d 00:00:00"), next_last.strftime("%Y-%m-%d 23:59:59")

def get_date_range():
    today = datetime.today()
    start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d 00:00:00")
    end_date = today.strftime("%Y-%m-%d 23:59:59")
    return start_date, end_date

def fetch_abcp_orders(start_date, end_date):
    log.info(f"📦 Получаем заказы из ABCP с {start_date} по {end_date}...")

    all_orders = []
    skip = 0
    limit = 1000

    try:
        # 1️⃣ Сначала получаем количество заказов
        count_params = {
            "userlogin": ABCP_USER,
            "userpsw": ABCP_PSW,
            "dateCreatedStart": start_date,
            "dateCreatedEnd": end_date,
            "format": "count"
        }
        resp_count = requests.get(ABCP_BASE_URL, params=count_params, timeout=30)
        resp_count.raise_for_status()
        total_count = int(resp_count.json().get("count", 0))
        log.info(f"🔢 Всего заказов для загрузки: {total_count}")

        # 2️⃣ Загружаем заказы постранично
        while skip < total_count:
            params = {
                "userlogin": ABCP_USER,
                "userpsw": ABCP_PSW,
                "format": "additional",
                "dateCreatedStart": start_date,
                "dateCreatedEnd": end_date,
                "limit": limit,
                "skip": skip,
                "desc": True
            }

            resp = requests.get(ABCP_BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            orders = resp.json()

            if not isinstance(orders, list) or not orders:
                break

            all_orders.extend(orders)
            log.info(f"🔄 Загрузили {len(orders)} заказов (всего: {len(all_orders)}/{total_count})")

            skip += limit

        log.info(f"✅ Всего заказов ABCP получено: {len(all_orders)}")
        with open("abcp_orders.json", "w", encoding="utf-8") as f:
            json.dump(all_orders, f, indent=2, ensure_ascii=False)
        return all_orders

    except Exception as e:
        log.error(f"❌ Ошибка получения заказов ABCP: {e}")
        return []

def fetch_versta_orders():
    log.info("📦 Получаем заказы из Versta24...")
    try:
        resp = requests.get(VERSTA_API_URL, headers=HEADERS_VERSTA)
        resp.raise_for_status()
        data = resp.json()
        orders = data.get("orders", [])
        log.info(f"✅ Заказы Versta24 сохранены: {len(orders)} шт.")
        with open("versta_orders.json", "w", encoding="utf-8") as f:
            json.dump(orders, f, indent=2, ensure_ascii=False)
        return orders
    except Exception as e:
        log.error(f"❌ Ошибка получения заказов Versta: {e}")
        return []

def extract_abcp_ids(orders):
    return {str(order.get("number")) for order in orders if order.get("number")}

def build_versta_order_map(orders):
    return {str(order.get("customerOrderId")): order for order in orders if order.get("customerOrderId")}

def build_abcp_status_map(orders):
    return {
        str(order.get("number")): (order["positions"][0]["status"] if order.get("positions") else "")
        for order in orders
    }

def analyze_matches(abcp_orders, versta_orders, test_order=None):
    abcp_ids = extract_abcp_ids(abcp_orders)
    versta_by_customer_id = build_versta_order_map(versta_orders)
    versta_ids = set(versta_by_customer_id.keys())

    if test_order:
        log.info(f"🔧 Тестовый заказ: {test_order}")
        abcp_ids = {str(test_order)} if str(test_order) in abcp_ids else set()
        versta_ids = {str(test_order)} if str(test_order) in versta_ids else set()
        versta_by_customer_id = {k: v for k, v in versta_by_customer_id.items() if k in versta_ids}

    common_ids = abcp_ids & versta_ids
    log.info(f"\n📊 Статистика сопоставления заказов:")
    log.info(f"— Заказов ABCP: {len(abcp_ids)}")
    log.info(f"— Заказов Versta: {len(versta_ids)}")
    log.info(f"✅ Совпавших заказов: {len(common_ids)}")

    return common_ids, versta_by_customer_id

def find_orders_for_update(common_ids, abcp_statuses, versta_by_customer_id):
    to_update = []
    for order_id in sorted(common_ids):
        abcp_status_name = abcp_statuses.get(order_id, "").strip()
        versta_order = versta_by_customer_id.get(order_id)

        master_status = versta_order.get("masterStatus")
        sub_status = versta_order.get("statusCode") or versta_order.get("subStatus")
        try:
            sub_status = int(sub_status) if sub_status is not None else None
        except ValueError:
            log.warning(f"⚠ subStatus не является числом: {sub_status} (order {order_id})")

        mapped_status = VERSTA_TO_ABCP.get((master_status, sub_status)) \
            or VERSTA_TO_ABCP.get((master_status, None)) \
            or VERSTA_TO_ABCP.get((versta_order.get("masterStatusName"), None))

        if not mapped_status:
            log.warning(f"⚠ Неизвестный статус: master={master_status}, sub={sub_status} для заказа {order_id}")
            continue

        if mapped_status["name"] != abcp_status_name:
            to_update.append({
                "order_id": order_id,
                "abcp_current": abcp_status_name,
                "versta_status": f"{versta_order.get('masterStatusName')} ({master_status}/{sub_status})",
                "abcp_new": mapped_status["name"],
                "abcp_new_id": mapped_status["id"]
            })
    return to_update

def update_abcp_order_positions(order, new_status_id):
    for i, pos in enumerate(order.get("positions", [])):
        pos_status = pos.get("status", "").strip()
        qty = float(pos.get("quantity", 0))
        sum_value = float(pos.get("priceOut", 0)) * qty
        pos_id = pos.get("id")

        if not pos_id or qty == 0 or sum_value == 0 or pos_status == "Отменен поставщиком":
            log.info(f"⏭ Пропуск позиции (id={pos_id}, qty={qty}, сумма={sum_value}, статус={pos_status})")
            continue

        payload = {
            "userlogin": ABCP_USER,
            "userpsw": ABCP_PSW,
            "order[number]": order["number"],
            f"order[positions][{i}][id]": pos_id,
            f"order[positions][{i}][statusCode]": new_status_id
        }
        try:
            resp = requests.post(ABCP_ORDER_UPDATE_URL, data=payload)
            resp.raise_for_status()
            result = resp.json()
            if "errors" in result:
                log.warning(f"⚠ Ошибка обновления позиции {pos_id} заказа {order['number']}: {result['errors']}")
            else:
                log.info(f"✅ Обновлена позиция {pos_id} в заказе {order['number']} → статус {new_status_id}")
        except Exception as e:
            log.error(f"❌ Ошибка при обновлении позиции {pos_id}: {e}")

# === Главная точка входа ===
if __name__ == "__main__":
    while True:
        try:
            log.info(f"\n=== 🔄 Запуск проверки {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

            TEST_MODE = False
            TEST_ORDER_NUMBER = None
            # TEST_ORDER_NUMBER = 205699637
            CUSTOM_START_DATE = "2025-03-01 00:00:00"
            CUSTOM_END_DATE = "2025-03-10 23:59:59"

            start, end = (CUSTOM_START_DATE, CUSTOM_END_DATE) if TEST_MODE else get_date_range()
            log.info(f"🔧 {'ТЕСТОВЫЙ' if TEST_MODE else 'Продакшн'} режим: даты с {start} по {end}")

            abcp_orders = fetch_abcp_orders(start, end)
            versta_orders = fetch_versta_orders()

            if not abcp_orders or not versta_orders:
                log.warning("🚫 Недостаточно данных для обработки.")
            else:
                common_ids, versta_by_customer_id = analyze_matches(abcp_orders, versta_orders, test_order=TEST_ORDER_NUMBER)
                abcp_statuses = build_abcp_status_map(abcp_orders)
                updates = find_orders_for_update(common_ids, abcp_statuses, versta_by_customer_id)

                log.info(f"\n🔄 Заказов для обновления статусов: {len(updates)}")

                if updates:
                    log.info("\n📋 Список заказов для обновления:")
                    for u in updates:
                        log.info(f"  ➤ Заказ {u['order_id']} | ABCP: {u['abcp_current']} → {u['abcp_new']} (ID {u['abcp_new_id']}) | Versta: {u['versta_status']}")
                        order_data = next((o for o in abcp_orders if str(o["number"]) == u["order_id"]), None)
                        if order_data:
                            update_abcp_order_positions(order_data, u["abcp_new_id"])
                        else:
                            log.warning(f"❌ Заказ {u['order_id']} не найден в списке ABCP для обновления.")
                else:
                    log.info("✅ Все заказы имеют актуальные статусы. Обновление не требуется.")

        except Exception as e:
            log.error(f"❌ Ошибка при выполнении: {e}")

        log.info("⏳ Ожидание 3 минуты до следующего запуска...\n")
        time.sleep(180)