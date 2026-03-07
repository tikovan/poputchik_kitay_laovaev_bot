
# Быстрый деплой на Ubuntu

```bash
sudo apt update
sudo apt install -y python3 python3-venv unzip

mkdir -p ~/china-courier-bot
cd ~/china-courier-bot

# загрузи сюда файлы проекта
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
nano .env

python bot.py
```

## systemd service

Создай файл:

```bash
sudo nano /etc/systemd/system/china-courier-bot.service
```

Вставь:

```ini
[Unit]
Description=China Courier Telegram Bot
After=network.target

[Service]
User=%i
WorkingDirectory=/home/%i/china-courier-bot
EnvironmentFile=/home/%i/china-courier-bot/.env
ExecStart=/home/%i/china-courier-bot/venv/bin/python /home/%i/china-courier-bot/bot.py
Restart=always

[Install]
WantedBy=multi-user.target
```

Потом:

```bash
sudo systemctl daemon-reload
sudo systemctl enable china-courier-bot.service
sudo systemctl start china-courier-bot.service
sudo systemctl status china-courier-bot.service
```
