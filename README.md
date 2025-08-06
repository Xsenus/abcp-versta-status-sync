# Скрипт синхронизации заказов ABCP и Versta24

Этот Python-скрипт синхронизирует статусы заказов между платформами ABCP и Versta24, получая заказы из обеих систем, сопоставляя их по идентификаторам заказов и обновляя статусы заказов в ABCP на основе комбинации `masterStatus` и `subStatus` из Versta24.

## Функциональность

- Получение заказов из API ABCP и Versta24.
- Сопоставление заказов по `customerOrderId` (Versta24) и `number` (ABCP).
- Точное сопоставление статусов на основе `masterStatus` и `subStatus` из Versta24 с соответствующими идентификаторами статусов ABCP.
- Обновление позиций заказов в ABCP новыми статусами при обнаружении расхождений.
- Поддержка тестового режима с указанным номером заказа и пользовательским диапазоном дат.
- Логирование операций в консоль и сохранение данных заказов в JSON-файлы (`abcp_orders.json`, `versta_orders.json`).
- Непрерывная работа с интервалом в 3 минуты между запусками.
- Поддержка запуска как службы systemd на Linux.

## Требования

- Python 3.6 или выше.
- Необходимые Python-библиотеки:
  - `requests`
  - `python-dotenv`
- Переменные окружения (указываются в файле `.env`):
  - `ABCP_BASE_URL`
  - `ABCP_ORDER_UPDATE_URL`
  - `VERSTA_API_URL`
  - `ABCP_USER`
  - `ABCP_PSW`
  - `VERSTA_TOKEN`
  - `LOG_LEVEL`

## Установка

1. Склонируйте или загрузите скрипт на свой компьютер.
2. Установите зависимости:

   ```bash
   pip install requests python-dotenv
   ```

3. Создайте файл `.env` и добавьте:

   ```env
   ABCP_BASE_URL=https://example.com/abcp/orders
   ABCP_ORDER_UPDATE_URL=https://example.com/abcp/update
   VERSTA_API_URL=https://api.versta24.com/orders
   ABCP_USER=your_abcp_username
   ABCP_PSW=your_abcp_password
   VERSTA_TOKEN=your_versta_token
   
   # Уровень логирования (выберите один из):
   # DEBUG | INFO | WARNING | ERROR | CRITICAL
   LOG_LEVEL=INFO
   ```

## Запуск

```bash
python sync_orders.py
```

Для тестового режима (определённый заказ и даты), включите в скрипте:

```python
TEST_MODE = True
TEST_ORDER_NUMBER = 123456789
CUSTOM_START_DATE = "2025-03-01 00:00:00"
CUSTOM_END_DATE = "2025-03-10 23:59:59"
```

## Сопоставление статусов Versta → ABCP

| Versta `masterStatus` | `subStatus`                     | ABCP ID | ABCP Название         |
|-----------------------|----------------------------------|---------|------------------------|
| 100                   | 6 CourierAssigned               | 405204  | Доставляется           |
| 100                   | 7 ParcelPickedUp                | 405204  | Доставляется           |
| 100                   | 8 CourierOnTheWay               | 405204  | Доставляется           |
| 100                   | 9 ParcelOnTheWay                | 405204  | Доставляется           |
| 100                   | 10 ParcelInTheRecipientCity     | 405204  | Доставляется           |
| 100                   | 11 ParcelHandedForDelivery      | 405204  | Доставляется           |
| 200                   | 12 ParcelInPickupPoint          | 405205  | Доставлен              |
| 700                   | 90 Finished                     | 385106  | Получен                |
| 800                   | 95 RefuseOfDelivery             | 385107  | Возвращается           |
| "ProblemDetected"     | Любой                           | 409382  | Проблема доставки      |
| "Returning"           | Любой                           | 384501  | Отменен поставщиком    |
| "Cancelled"           | Любой                           | 404714  | Отменен по неоплате    |
| "NotDelivered"        | Любой                           | 418655  | Вернулся в ТК          |
| "ReturnAccepted"      | Любой                           | 418656  | Возврат принят         |
| 0                     | None                            | 409381  | Новый                  |

## Логирование

Формат логов:

```text
2025-08-06 23:49:15 [WARNING] ⚠ Неизвестный статус: master=300, sub=None для заказа 221911169
2025-08-06 23:49:17 [INFO]   ➤ Заказ 221260161 | ABCP: Отправлен → Новый (ID 409381) | Versta: Новый заказ (0/None)
```

## Файлы

- `abcp_orders.json` — все заказы из ABCP.
- `versta_orders.json` — все заказы из Versta.

## Запуск как systemd-служба (Linux)

<!-- markdownlint-disable MD029 -->
1. Создайте unit-файл `/etc/systemd/system/abcp_versta_sync.service`:

   ```ini
   [Unit]
   Description=ABCP-Versta Order Sync Service
   After=network.target

   [Service]
   ExecStart=/usr/bin/python3 /home/youruser/projects/abcp_versta_sync/sync_orders.py
   WorkingDirectory=/home/youruser/projects/abcp_versta_sync/
   Environment="PYTHONUNBUFFERED=1"
   Restart=always
   RestartSec=10

   [Install]
   WantedBy=multi-user.target
   ```

2. Перезапустите systemd и включите службу:

   ```bash
   sudo systemctl daemon-reexec
   sudo systemctl daemon-reload
   sudo systemctl enable abcp_versta_sync
   sudo systemctl start abcp_versta_sync
   ```

3. Проверка статуса:

   ```bash
   sudo systemctl status abcp_versta_sync
   journalctl -u abcp_versta_sync -f
   ```
<!-- markdownlint-enable MD029 -->
