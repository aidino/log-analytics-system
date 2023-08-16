
```bash
sudo chown root:root filebeat.yml
sudo chmod 644 filebeat.yml
sudo chown -R $USER /log
```

----> Generate logs up to 10MB and split log files every 1MB in "web/log/*.log" path with "apache combined" format

```bash
./flog_linux -t log -f apache_common -o log/apache.log -d 1s -w -l
```
