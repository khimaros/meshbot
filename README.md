# MESHBOT

a humble bot for meshtastic

## features

- simple TOML based configuration with sane defaults
- OpenAI API compatible (including llama.cpp) LLM generated responses
- fully customizeable channel activity level (responses, idle messages, etc)
- simple SYN/ACK responses for quick testing
- responds to "Alert Bell" with receive date and user's last known position
- send direct "Copy" message on received packet (at user request)
- can respond only to direct messages or with customizable channel
- maintain unabridged raw packet logs for later use
- minimal dependencies and relatively small codebase

## getting started

```
git clone https://github.com/khimaros/meshbot/

cd ./meshbot/

pip install -r requirements.txt

cp ./config{.example,}.toml

./meshbot.py
```

for a more reproducible environment, use [mise](https://github.com/jdx/mise/)

## acknowledgements

i made use of Cline + Claude 4 Sonnet for some code changes, but monitored the
output closely to ensure it was producing sane code
