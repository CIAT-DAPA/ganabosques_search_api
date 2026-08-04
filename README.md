# 🐮 Ganabosques Search API
![GitHub release](https://img.shields.io/github/v/release/CIAT-DAPA/ganabosques_search_api)
![GitHub tag](https://img.shields.io/github/v/tag/CIAT-DAPA/ganabosques_search_api)

## 📌 Descripción
Ganabosques Search API es un servicio backend para búsqueda, consulta y agregación de datos del ecosistema Ganabosques. Expone catálogos administrativos, fincas, empresas, movimientos, áreas protegidas, deforestación, análisis de riesgo y utilidades de autenticación contra Keycloak.

La aplicación está construida con FastAPI, persiste en MongoDB mediante MongoEngine/PyMongo y centraliza el enrutado en `src/main.py`. El arranque inicializa la conexión a MongoDB, carga variables de entorno y registra todos los routers disponibles. Varios endpoints usan validación de token JWT y control de permisos.

## 🎯 Rol en el ecosistema
Este servicio es el **punto de acceso programático** del sistema Ganabosques. Su misión es exponer de forma segura y estandarizada los resultados de los cálculos de alertas y la información almacenada en la base de datos, permitiendo que aplicaciones externas —como la App— consulten e integren los indicadores de alertas en sus propios procesos de análisis, monitoreo y toma de decisiones.

En términos prácticos, actúa como el **puente entre la Database Ganabosques y las interfaces cliente** (App y consumidores externos), garantizando la interoperabilidad del sistema. Entre sus responsabilidades principales:

- Exponer servicios REST para consultar alertas a nivel de predio, vereda y empresa.
- Gestionar solicitudes de aplicaciones externas, garantizando seguridad y control de acceso.
- Validar tokens JWT emitidos por el servicio de autenticación (Keycloak).
- Transformar y entregar datos en formatos estructurados y consistentes (JSON).
- Registrar logs de consultas para trazabilidad y auditoría.

## 🏗️ Estructura del proyecto
```text
ganabosques_search_api/
├── .github/
│   └── workflows/
│       └── pipeline.yaml
├── pipelines/
│   └── pipeline-test.yml
├── Dockerfile
├── DOCKER_GUIE.md
├── Jenkinsfile
├── README.md
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── database.py
│   ├── requirements.txt
│   ├── auth/
│   │   ├── auth.py
│   │   ├── get_client_token.py
│   │   ├── token_validation_router.py
│   │   └── utils.py
│   ├── dependencies/
│   │   ├── auth_guard.py
│   │   └── permissions_groups.py
│   ├── routes/
│   │   ├── adm1.py
│   │   ├── adm2.py
│   │   ├── adm3.py
│   │   ├── adm3Front.py
│   │   ├── adm3risk.py
│   │   ├── adm3risk_by_analysis_and_adm3.py
│   │   ├── adm3risk_get_all.py
│   │   ├── analisys_risk_router.py
│   │   ├── analysis.py
│   │   ├── base_route.py
│   │   ├── deforestation.py
│   │   ├── enterprise.py
│   │   ├── enterprise_risk.py
│   │   ├── enum.py
│   │   ├── farm.py
│   │   ├── farmingareas.py
│   │   ├── farmpolygons.py
│   │   ├── farmrisk.py
│   │   ├── farmrisk_paginated.py
│   │   ├── farmriskverification.py
│   │   ├── get_analysis.py
│   │   ├── movement.py
│   │   ├── protectedareas.py
│   │   └── suppliers.py
│   ├── schemas/
│   │   ├── extid_schema.py
│   │   └── logschema.py
│   └── tools/
│       ├── endpoints.py
│       ├── logger.py
│       ├── pagination.py
│       └── utils.py
└── tests/
    ├── test_auth.py
    ├── test_token_validation_router.py
    ├── test_base_route.py
    ├── test_adm1.py
    ├── test_adm2.py
    ├── test_adm3.py
    ├── test_analysis.py
    ├── test_farm.py
    ├── test_enterprise.py
    ├── test_movement.py
    └── ...
```

> Nota: el workspace también contiene `.env` y `api.log`, pero son artefactos locales del entorno y no forman parte del código fuente de la API.

## ⚙️ Requisitos
- Python 3.10.X 
- `pip`
- Dependencias clave fijadas en `src/requirements.txt`:
  - FastAPI 0.115.12
  - Uvicorn 0.34.3
  - Pydantic 2.11.5
  - MongoEngine 0.29.1
  - PyMongo 4.13.0
  - python-dotenv 1.1.0
  - python-jose 3.5.0
  - requests 2.32.4
  - httpx 0.28.1
  - `ganabosques_orm` desde GitHub (`git+https://github.com/CIAT-DAPA/ganabosques_orm`)

## 🔐 Variables de entorno
Las variables consumidas por el código son las siguientes:

| Variable | Ejemplo | Descripción |
| --- | --- | --- |
| `MONGO_URI` | `mongodb://localhost:27017` | URI de conexión a MongoDB usada por `src/database.py`. |
| `MONGO_DB_NAME` | `ganabosques` | Nombre de la base de datos MongoDB. |
| `KEYCLOAK_URL` | `http://localhost:8080` | Base URL de Keycloak para login, JWKS y validación de tokens. |
| `KEYCLOAK_REALM` | `GanaBosques` | Realm de Keycloak usado por autenticación y validación. |
| `KEYCLOAK_CLIENT_ID` | `GanaBosques` | Client ID usado por login y validación. |
| `KEYCLOAK_CLIENT_SECRET` | `CHANGE_ME` | Client secret usado por `/auth/login`. |

## 🚀 Instalación
Puedes correr este servicio de dos formas: **de forma local**, usando un entorno virtual de Python (útil para desarrollo y debugging día a día), o **con Docker**, construyendo la imagen del contenedor directamente desde este repositorio (útil para réplicas de entornos de prueba o producción). Elige la que se ajuste a lo que necesitas — ambas terminan exponiendo la API en el mismo puerto.

### Entorno local (entorno virtual de Python)
Para este modo necesitas tener Python 3.10 instalado en tu máquina. El proyecto usa un entorno virtual (`venv`) para aislar sus dependencias del resto del sistema, así que el flujo es: clonar, crear el entorno virtual, activarlo e instalar dependencias desde `src/requirements.txt`.

```bash
git clone https://github.com/CIAT-DAPA/ganabosques_search_api.git
cd ganabosques_search_api
python -m venv env
```

Activar el entorno virtual:

Windows:
```bash
env\Scripts\activate
```

Linux/macOS:
```bash
source env/bin/activate
```

Instalar dependencias:
```bash
pip install -r src/requirements.txt
```

### Con Docker
Si prefieres no instalar Python ni gestionar el entorno virtual localmente, puedes construir y correr la API directamente como contenedor usando el `Dockerfile` incluido en este repositorio. Solo necesitas Docker instalado y un archivo `.env` con las variables descritas arriba.

```bash
docker build -t ganabosques-search-api .
docker run --env-file .env -p 8000:8000 ganabosques-search-api
```

Para más detalle sobre el flujo con Docker, revisa [DOCKER_GUIE.md](DOCKER_GUIE.md).

## ▶️ Ejecutar la API
Una vez instaladas las dependencias (local) o construida la imagen (Docker), levantas la API con:

```bash
uvicorn src.main:app --reload
```

### Producción
Para producción se ejecuta sin `--reload`, enlazando a todas las interfaces de red (`0.0.0.0`) y usando el puerto definido `APP_PORT`. El proceso corre en segundo plano y los logs de la API se redirigen al archivo `app.log`:

```bash
uvicorn src.main:app --host 0.0.0.0 --port $APP_PORT > app.log 2>&1 &
```

La API queda disponible en `http://127.0.0.1:8000` y la documentación interactiva en:

- `http://127.0.0.1:8000/docs`
- `http://127.0.0.1:8000/redoc`


## 📡 Endpoints principales
| Método | Ruta | Descripción |
| --- | --- | --- |
| `POST` | `/auth/login` | Obtiene access y refresh tokens usando el password grant de Keycloak. |
| `POST` | `/auth/get-client-token` | Obtiene un token de cliente con `client_credentials`. |
| `GET` | `/auth/token/validate` | Valida el bearer token, consulta JWKS, sincroniza el usuario local y devuelve el payload filtrado. |
| `GET` | `/adm1/`, `/adm1/by-ids`, `/adm1/by-name`, `/adm1/by-extid`, `/adm1/paged/` | Catálogo de primer nivel administrativo con filtros por ID, nombre, ext-id y paginación. |
| `GET` | `/adm2/`, `/adm2/by-ids`, `/adm2/by-name`, `/adm2/by-extid`, `/adm2/by-adm1`, `/adm2/paged/` | Catálogo de segundo nivel administrativo. |
| `GET` | `/adm3/`, `/adm3/by-ids`, `/adm3/by-name`, `/adm3/by-extid`, `/adm3/by-adm2`, `/adm3/by-label`, `/adm3/paged/` | Catálogo de tercer nivel administrativo. |
| `GET` | `/farm/`, `/farm/by-ids`, `/farm/by-extid`, `/farm/by-adm3`, `/farm/paged/` | Consulta de fincas con filtros y paginación. |
| `GET` | `/enterprise/`, `/enterprise/by-ids`, `/enterprise/by-name`, `/enterprise/by-extid`, `/enterprise/by-adm2`, `/enterprise/paged/` | Consulta de empresas con filtros y paginación. |
| `GET` | `/deforestation/`, `/protectedareas/`, `/farmingareas/`, `/analysis/` | Catálogos espaciales y de análisis base, cada uno con filtros y paginación propios. |
| `GET` | `/movement/`, `/movement/by-ids`, `/movement/by-extid`, `/movement/by-farmid`, `/movement/by-enterpriseid`, `/movement/statistics-by-farmid`, `/movement/statistics-by-enterpriseid`, `/movement/paged/` | Movimientos de ganado con agregaciones y estadísticas. |
| `GET` | `/enums/?enum_name=...` | Devuelve los valores de un enum del paquete `ganabosques_orm`. |
| `POST` | `/farmrisk/by-analysis-and-farm`, `/farmrisk/by-analysis-id`, `/farmriskverification/` | Riesgo de finca y verificación de riesgo. |
| `POST` | `/adm3risk/by-adm3-and-type`, `/adm3risk/by-analysis-and-adm3`, `/risk/by-ids-and-type`, `/enterprise-risk/details/by-enterprise` | Agregaciones y consultas de riesgo por división administrativa y empresa. |
| `GET` | `/analysis/by-deforestation` | Busca análisis asociados a una capa de deforestación. |

La mayoría de los endpoints de negocio están protegidos con `HTTPBearer` y validación de permisos vía `src/dependencies/auth_guard.py`.
para conocer los parametros y firmas de usar cada endpoint, revisa la documentación interactiva en `/docs` o `/redoc`.

## 🔒 Autenticación
La autenticación se apoya en Keycloak y está separada en tres flujos:

1. `/auth/login` usa el flujo password grant. Recibe `username` y `password`, construye la petición al endpoint de tokens de Keycloak y devuelve la respuesta original con access/refresh token.
2. `/auth/get-client-token` usa `client_credentials` para obtener un token de servicio a partir de `client_id` y `client_secret`.
3. `/auth/token/validate` recibe un bearer token, lee el `kid` del header JWT, descarga las claves públicas JWKS desde Keycloak, valida firma, audiencia e issuer, y luego busca o crea el usuario local en Mongo por `sub`.

## 🧪 Testing
El comando que ejecuta la CI actual es:

```bash
python -m unittest discover tests
```

Ese mismo flujo se usa en GitHub Actions (`.github/workflows/pipeline.yaml`) y en el pipeline de Azure DevOps (`pipelines/pipeline-test.yml`). El repositorio también contiene una cobertura amplia de pruebas unitarias para auth, validación de tokens, routers de catálogos, rutas de riesgo, movimientos y paginación.

## 👥 Mantenedores / Licencia
Mantenedores: [CIAT-DAPA](https://github.com/CIAT-DAPA) / Alliance Bioversity-CIAT

- [stevensotelo](https://github.com/stevensotelo) 
- [victor-993](https://github.com/victor-993)
