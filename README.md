```bash
# поднимаем 8000-8002
curl -X GET http://localhost:8001/read -H "Content-Type: application/json" -d '{"key": 0}'
curl -X POST http://localhost:8001/create -H "Content-Type: application/json" -d '{"key": 0}'
curl -X GET http://localhost:8001/read -H "Content-Type: application/json" -d '{"key": 0}'
curl -X PUT http://localhost:8001/update -H "Content-Type: application/json" -d '{"key": 0, "value": 1}'
curl -X GET http://localhost:8001/read -H "Content-Type: application/json" -d '{"key": 0}'
curl -X POST http://localhost:8001/delete -H "Content-Type: application/json" -d '{"key": 0}'
curl -X GET http://localhost:8001/read -H "Content-Type: application/json" -d '{"key": 0}'
# поднимаем 8003 и 8004, удаляем две ноды из 8000-8002, из них одного лидера
curl http://localhost:8000/status
curl http://localhost:8001/status
curl http://localhost:8002/status
curl http://localhost:8003/status
curl http://localhost:8004/status
```
