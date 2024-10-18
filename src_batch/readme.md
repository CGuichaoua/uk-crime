A faire avant de lancer le script "batch_mariadb"
- arrêter mysql sur la console XAMPP
- modifier "C:\xampp\mysql\bin\my.ini"
[mysqld]
wait_timeout = 28800
interactive_timeout = 28800
max_allowed_packet = 64M
- relancer mysql