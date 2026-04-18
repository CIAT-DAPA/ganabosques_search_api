from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pymongo.errors import ServerSelectionTimeoutError
from src.database import init_db
from dotenv import load_dotenv
from src.auth.auth import router as auth_router
from src.auth.get_client_token import router as get_client_token_router
from src.auth.token_validation_router import router as validate_token_router
from src.routes.adm1 import router as adm1
from src.routes.adm2 import router as adm2
from src.routes.adm3 import router as adm3
from src.routes.farm import router as farm
from src.routes.farmpolygons import router as farmpolygons
from src.routes.enterprise import router as enterprise
from src.routes.suppliers import router as suppliers
from src.routes.deforestation import router as deforestation
from src.routes.protectedareas import router as protectedareas
from src.routes.farmingareas import router as farmingareas
from src.routes.analysis import router as analysis
from src.routes.adm3risk import router as adm3risk
from src.routes.farmrisk import router as farmrisk
from src.routes.farmriskverification import router as farmriskverification
#from src.routes.enterpriserisk import router as enterpriserisk
from src.routes.movement import router as movement
from src.routes.analisys_risk_router import router as get_farmrisk_by_analyses
from src.routes.get_analysis import router as get_analysis_router
from src.routes.adm3risk_by_analysis_and_adm3 import router as adm3risk_by_analysis_and_adm3
from src.routes.adm3risk_get_all import router as adm3risk_get_all
from src.routes.enterprise_risk import router as enterprise_risk_router
from src.routes.farmrisk_paginated import router as farmrisk_paginated
from src.routes.adm3Front import router as adm3Front
from src.routes.enum import router as enum
from src.tools.logger import logger


app = FastAPI(
    title="Ganabosques search api"
)

load_dotenv()

try:
    init_db()
    logger.info("Conexión a MongoDB exitosa")
except ServerSelectionTimeoutError as e:
    logger.exception("No se pudo conectar con MongoDB al iniciar")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(ServerSelectionTimeoutError)
async def db_connection_error_handler(request: Request, exc: ServerSelectionTimeoutError):
    return JSONResponse(
        status_code=503,
        content={"detail": "Error de conexión con la base de datos. Verifica si el servidor está en línea."},
    )

# Auth
app.include_router(auth_router)
app.include_router(get_client_token_router)
app.include_router(validate_token_router)

# Administrative levels
app.include_router(adm1)
app.include_router(adm2)
app.include_router(adm3)

# Farm and Enterprise
app.include_router(farm)
app.include_router(farmpolygons)
app.include_router(enterprise)
app.include_router(suppliers)

# Spatial data
app.include_router(deforestation)
app.include_router(protectedareas)
app.include_router(farmingareas)

# Analysis risk
app.include_router(analysis)
app.include_router(adm3risk)
app.include_router(farmrisk)
app.include_router(farmriskverification)
#app.include_router(enterpriserisk)

# Movements
app.include_router(movement)

# Enumns
app.include_router(enum)
app.include_router(get_farmrisk_by_analyses)
app.include_router(get_analysis_router)
app.include_router(adm3risk_by_analysis_and_adm3)
app.include_router(adm3risk_get_all)
app.include_router(enterprise_risk_router)
app.include_router(farmrisk_paginated)
app.include_router(adm3Front)
# uvicorn main:app --reload