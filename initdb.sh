#! /bin/sh

# initdb -D "/home/backuper/git/Итоговая аттестация/Project3/db"
# pg_ctl -D "/home/backuper/git/Итоговая аттестация/Project3/db" -l ./db/logfile start
psql postgres -f initdb.sql
