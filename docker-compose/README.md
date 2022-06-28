In order to run elasticsearch on windows:
1. Open powershell
2. wsl.exe -u root
3. sysctl -w vm.max_map_count=262144
4. docker-compose up