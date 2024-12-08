curl -X POST http://localhost:8000/create -H "Content-Type: application/json" -d '{"key": 0}'
curl -X POST http://localhost:8000/update -H "Content-Type: application/json" -d '{"key": 0, "value": 1}'
curl -X POST http://localhost:8000/read -H "Content-Type: application/json" -d '{"key": 0}'            
