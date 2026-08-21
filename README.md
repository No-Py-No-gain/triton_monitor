# Documentación técnica y diagramas de arquitectura

triton_monitor/
├── src/
│   ├── triton_telemetry/
│   │   ├── __init__.py         # Expone la API pública del paquete mediante __all__
│   │   ├── exceptions.py       # Excepciones semánticas custom de Triton (no BaseException)
│   │   ├── sanitizer.py        # Validación declarativa con argparse (callables custom)
│   │   ├── core.py             # Lógica asíncrona de consulta paralela (asyncio.TaskGroup)
│   │   └── logging_engine.py   # Formateador JSON avanzado y pipeline asíncrono no bloqueante
│   └── app_operator.py         # Punto de entrada CLI ejecutable (argparse + except*)
└── requirements.txt            # Dependencias aisladas del proyecto