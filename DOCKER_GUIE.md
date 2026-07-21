# Guía de Containerización con Docker - Ganabosques Search API

## 📋 Descripción General

Esta API está desarrollada con FastAPI y MongoDB, y ha sido containerizada para facilitar su despliegue en diferentes entornos (desarrollo, pruebas y producción).

### Características

* Dockerfile optimizado para producción
* Configuración mediante variables de entorno
* Conexión a MongoDB externa
* Logs visibles mediante Docker
* Compatible con Docker Compose
* Independiente de Conda o entornos virtuales locales

---

# 🚀 Construcción de la Imagen

Desde la raíz del proyecto:

```bash
docker build -t ganabosques-api .
```

Verificar imagen creada:

```bash
docker images | grep ganabosques-api
```

---

# 🔧 Variables de Entorno

La aplicación utiliza variables de entorno para su configuración.

## Archivo .env de ejemplo

```env
DEBUG=true

MONGO_URI=mongodb://host.docker.internal:27017
MONGO_DB_NAME=ganabosques2

KEYCLOAK_URL=https://keycloak.example.com
KEYCLOAK_REALM=GanaBosques
KEYCLOAK_CLIENT_ID=GanaBosques
KEYCLOAK_CLIENT_SECRET=xxxxxxxxxxxxxxxx
```

## Variables disponibles

| Variable               | Descripción                     |
| ---------------------- | ------------------------------- |
| DEBUG                  | Activa modo debug               |
| MONGO_URI              | Cadena de conexión MongoDB      |
| MONGO_DB_NAME          | Nombre de la base de datos      |
| KEYCLOAK_URL           | URL del servidor Keycloak       |
| KEYCLOAK_REALM         | Realm de Keycloak               |
| KEYCLOAK_CLIENT_ID     | Cliente configurado en Keycloak |
| KEYCLOAK_CLIENT_SECRET | Secret del cliente              |

---

# 🐳 Ejecución del Contenedor

## Utilizando archivo .env

```bash
docker run -d \
  --name ganabosques-api \
  -p 8000:8000 \
  --env-file .env \
  ganabosques-api
```

---

## Utilizando variables individuales

```bash
docker run -d \
  --name ganabosques-api \
  -p 8000:8000 \
  -e MONGO_URI=mongodb://host.docker.internal:27017 \
  -e MONGO_DB_NAME=ganabosques2 \
  -e KEYCLOAK_URL=https://keycloak.example.com \
  -e KEYCLOAK_REALM=GanaBosques \
  -e KEYCLOAK_CLIENT_ID=GanaBosques \
  -e KEYCLOAK_CLIENT_SECRET=xxxxxxxx \
  ganabosques-api
```

---

# 🌐 Acceso a la API

Una vez iniciada:

```text
http://localhost:8000
```

Documentación Swagger:

```text
http://localhost:8000/docs
```

Documentación ReDoc:

```text
http://localhost:8000/redoc
```

---

# 🗄️ Configuración de MongoDB

## Importante

Dentro de Docker:

```text
localhost
```

hace referencia al propio contenedor.

Por esta razón, si MongoDB está fuera del contenedor, NO debe utilizarse:

```env
MONGO_URI=mongodb://localhost:27017
```

### Ejemplo para Mongo local en Windows/Mac

```env
MONGO_URI=mongodb://host.docker.internal:27017
```

### Ejemplo para Mongo remoto

```env
MONGO_URI=mongodb://192.168.1.100:27017
```

o

```env
MONGO_URI=mongodb://mongo.miempresa.com:27017
```

---

# 📜 Logs

Los logs de la aplicación se envían a:

* Consola del contenedor
* Docker Logs

Consultar logs:

```bash
docker logs ganabosques-api
```

Logs en tiempo real:

```bash
docker logs -f ganabosques-api
```

Últimas 100 líneas:

```bash
docker logs --tail 100 ganabosques-api
```

---

# 🔎 Verificación de Estado

Ver contenedor:

```bash
docker ps
```

Inspeccionar:

```bash
docker inspect ganabosques-api
```

Ver variables cargadas:

```bash
docker exec -it ganabosques-api env
```

Verificar conexión API:

```bash
curl http://localhost:8000/docs
```

---

# 🛠️ Acceso al Contenedor

Entrar al contenedor:

```bash
docker exec -it ganabosques-api sh
```

Ver archivos:

```bash
ls -la
```

---

# 🔄 Actualización de la Aplicación

Detener contenedor:

```bash
docker stop ganabosques-api
```

Eliminar contenedor:

```bash
docker rm ganabosques-api
```

Reconstruir imagen:

```bash
docker build --no-cache -t ganabosques-api .
```

Ejecutar nuevamente:

```bash
docker run -d \
  --name ganabosques-api \
  -p 8000:8000 \
  --env-file .env \
  ganabosques-api
```

---

# 🧹 Limpieza

Eliminar contenedores detenidos:

```bash
docker container prune
```

Eliminar imágenes no utilizadas:

```bash
docker image prune -a
```

Eliminar volúmenes no utilizados:

```bash
docker volume prune
```

---


# 🚨 Solución de Problemas

## Error de conexión MongoDB

Ejemplo:

```text
ServerSelectionTimeoutError
```

Verificar:

```bash
docker exec -it ganabosques-api env | grep MONGO
```

Confirmar conectividad:

```bash
docker exec -it ganabosques-api sh
```

y luego:

```bash
ping host.docker.internal
```

---

## La API no inicia

Consultar logs:

```bash
docker logs ganabosques-api
```

---

## Puerto ocupado

Ejecutar en otro puerto:

```bash
docker run -d \
  --name ganabosques-api \
  -p 5001:8000 \
  --env-file .env \
  ganabosques-api
```

Acceso:

```text
http://localhost:5001/docs
```


