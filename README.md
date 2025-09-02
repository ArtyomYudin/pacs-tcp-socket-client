# Зайди в контейнер
docker exec -it rabbitmq bash

# Проверь пользователей
rabbitmqctl list_users

# Проверь виртуальные хосты
rabbitmqctl list_vhosts

# Добавь пользователя (если его нет)
rabbitmqctl add_user pacs_tcp_client "97OUWipH4txB"

# Create a virtual host with metadata
rabbitmqctl add_vhost it_support --description "IT Support Portal" --default-queue-type quorum

# Добавь права на виртуальный хост it_support
rabbitmqctl set_permissions -p it_support pacs_tcp_client ".*" ".*" ".*"