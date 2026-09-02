# =============================================================================
# módulo: sanitizer.py — Validación de parámetros CLI (argparse + re)
# =============================================================================
# ROL EN LA ARQUITECTURA:
#   Este módulo es la "frontera de entrada" del paquete triton_telemetry.
#   Se encarga de validar y sanitizar los argumentos que el usuario escribe en
#   la línea de comandos ANTES de que lleguen a tocar la red (core.py) o el
#   event loop. Intercepta datos corruptos o fuera de rango de forma temprana.
#
#   Contiene DOS funciones "callable" que se inyectan directamente en argparse
#   (ver app_operator.py: parser.add_argument(..., type=parse_timeout) y
#   type=parse_cluster_id). De esa forma, la propia librería argparse las
#   invoca con el string tipeado por el usuario y, si lanzan
#   argparse.ArgumentTypeError, la CLI sale limpiamente con código de error 2.
#
#   La validación es DECLARATIVA y ESTRICTA:
#     - parse_timeout     -> rango flotante [0.1, 5.0] segundos.
#     - parse_cluster_id  -> patrón regex cluster-<region>-<numero>.
#
# RELACIONES DEL PAQUETE:
#   - app_operator.py -> inyecta ambas funciones como "type=" de argparse.
#   - exceptions.py    -> NO depende de este módulo, pero comparten el mismo
#                         espíritu de "fronteras robustas" del integrante 1.
#   - core.py          -> usa el valor ya validado que devuelve parse_timeout.
# =============================================================================
# Descripcion: Validación declarativa estricta en la frontera. Intercepta datos corruptos
# o fuera de rango de dominio antes de que interactúen con el bucle de eventos o los hilos de red.

import argparse   # Proporciona ArgumentTypeError: la excepción que, al lanzarse
                  # dentro de un "type=", hace que argparse imprima el mensaje de
                  # ayuda y SALGA del programa con código de error del sistema 2.
import re         # Expresiones regulares: usadas para validar con precisión el
                  # formato formal del identificador de clúster.


# ---------------------------------------------------------------------------
# Validador CLI de Tiempos de Espera (--timeout)
# ---------------------------------------------------------------------------
# Estructura: argparse llama a esta función con el string crudo que el usuario
# tecleó (p.ej. "2.5" o "abc"). Nosotros lo convertimos a float y comprobamos
# el rango. Si algo falla, lanzamos ArgumentTypeError para que la CLI salga
# con código 2 (convención de error de sistema para "uso incorrecto").
def parse_timeout(value: str) -> float:
    """
    Sanitiza y valida el tiempo de espera (timeout) para las peticiones HTTP.
    Debe ser un flotante estrictamente en el rango [0.1, 5.0] segundos.

    - Entradas numéricas válidas devuelven el float correspondiente.
    - Entradas fuera de rango o NO numéricas lanzan argparse.ArgumentTypeError,
      lo que obliga a argparse a mostrar la ayuda y salir con código de error 2.
    """
    try:
        # float(value) convierte el string a número. Convierte correctamente
        # "0.1" -> 0.1, pero lanza ValueError si no es numérico ("abc", "", etc).
        val = float(value)
        # Comprobación de rango INCLUSIVO: [0.1, 5.0]. Fuera de ese intervalo
        # se rechaza la entrada por considerarse un timeout irreal.
        if not (0.1 <= val <= 5.0):
            raise ValueError("El timeout debe estar entre 0.1 y 5.0 segundos.")
        # Entrada válida: la devolvemos como float para que argparse la use.
        return val
    except ValueError as e:
        # UNIFICAMOS el manejo: tanto el "no numérico" (float lanzó ValueError)
        # como el "fuera de rango" (nosotros lanzamos ValueError) caen aquí.
        # argparse requiere ArgumentTypeError (no ValueError) para disparar el
        # mensaje de ayuda y salir limpiamente con código de error 2.
        raise argparse.ArgumentTypeError(f"Timeout inválido '{value}': {str(e)}")


# ---------------------------------------------------------------------------
# Validador de Identificadores de Clúster (--cluster-id)
# ---------------------------------------------------------------------------
# Se valida con una expresión regular (re). La regex exige la estructura:
#   cluster-<region>-<numero>
# donde:
#   - "cluster-" es el prefijo fijo e inamovible.
#   - <region> es un bloque de letras minúsculas (2 a 10) que puede ir seguido
#     OPCIONALMENTE de "-" + otra palabra en minúsculas (sub-región), para
#     aceptar tanto "cluster-nyc-01" como "cluster-us-east-01".
#   - <numero> son EXACTAMENTE dos dígitos (01, 02, ..., 99).
def parse_cluster_id(value: str) -> str:
    """
    Valida que el identificador del clúster siga el patrón formal de expresión
    regular: cluster-<region>-<numero> (e.g., cluster-us-east-01 o cluster-nyc-01).
    """
    # ^        -> ancla: el string debe EMPEZAR aquí (nada antes del prefijo).
    # cluster- -> el sufijo literal "cluster-".
    # [a-z]{2,10} -> el primer bloque de la región: letras minúsculas (2 a 10).
    # (?:-[a-z]+)? -> GRUPO OPCIONAL: si viene "-" lo debe seguir 1+ letras
    #                 minúsculas. Es lo que hace válido "us-east" (sub-región)
    #                 pero también permite regiones simples como "nyc".
    #                 El "(?:...)" es un grupo NO capturador (no guarda valor).
    # -         -> el guion separador antes del número.
    # \d{2}     -> EXACTAMENTE dos dígitos decimales.
    # $         -> ancla: el string debe TERMINAR aquí (nada después del número).
    pattern = r"^cluster-[a-z]{2,10}(?:-[a-z]+)?-\d{2}$"
    # re.match() comprueba el patrón desde el inicio del string. Si no coincide
    # (None), la entrada es inválida y argumentamos el rechazo ante argparse.
    if not re.match(pattern, value):
        raise argparse.ArgumentTypeError(
            f"El ID del clúster '{value}' no cumple con el formato requerido "
            f"(ejemplo válido: 'cluster-us-east-01')."
        )
    # Válido: devolvemos el string tal cual (no hace falta transformarlo).
    return value


# ---------------------------------------------------------------------------
# BLOQUE DE ARRANQUE PARA PRUEBA RÁPIDA (NO usado cuando se importa el paquete)
# ---------------------------------------------------------------------------
# Al igual que en core.py, este bloque solo corre cuando ejecutamos
# "python sanitizer.py" directamente. Al importar (vía app_operator) __name__ no
# es "__main__" y aquí NO se ejecuta nada. Sirve como batería de pruebas visual
# para validar ambas funciones sin montar la app completa.
if __name__ == "__main__":
    import sys

    def probar_timeout(casos_validos, casos_invalidos):
        print("=== BATERÍA DE PRUEBAS: parse_timeout (rango [0.1, 5.0]) ===")
        ok = 0
        total = len(casos_validos) + len(casos_invalidos)
        for entrada in casos_validos:
            try:
                resultado = parse_timeout(entrada)
                print(f"[OK]    '{entrada}' -> {resultado}")
                ok += 1
            except argparse.ArgumentTypeError as e:
                print(f"[ERROR] '{entrada}' debía ser VÁLIDO pero se rechazó: {e}")
        for entrada in casos_invalidos:
            try:
                resultado = parse_timeout(entrada)
                print(f"[ERROR] '{entrada}' debía ser INVÁLIDO pero se aceptó: {resultado}")
            except argparse.ArgumentTypeError:
                print(f"[OK]    '{entrada}' -> rechazado correctamente")
                ok += 1
        print(f"  -> Resumen timeout: {ok}/{total} casos correctos\n")
        return ok, total

    def probar_cluster(casos_validos, casos_invalidos):
        print("=== BATERÍA DE PRUEBAS: parse_cluster_id (cluster-<region>-<num>) ===")
        ok = 0
        total = len(casos_validos) + len(casos_invalidos)
        for entrada in casos_validos:
            try:
                resultado = parse_cluster_id(entrada)
                print(f"[OK]    '{entrada}' -> validado")
                ok += 1
            except argparse.ArgumentTypeError as e:
                print(f"[ERROR] '{entrada}' debía ser VÁLIDO pero se rechazó: {e}")
        for entrada in casos_invalidos:
            try:
                resultado = parse_cluster_id(entrada)
                print(f"[ERROR] '{entrada}' debía ser INVÁLIDO pero se aceptó: {resultado}")
            except argparse.ArgumentTypeError:
                print(f"[OK]    '{entrada}' -> rechazado correctamente")
                ok += 1
        print(f"  -> Resumen cluster: {ok}/{total} casos correctos\n")
        return ok, total

    ok1, total1 = probar_timeout(
        casos_validos=["0.1", "1.0", "2.5", "5.0"],
        casos_invalidos=["0.0", "5.1", "-1", "abc", "", "2,5", "10"],
    )
    ok2, total2 = probar_cluster(
        casos_validos=["cluster-us-east-01", "cluster-nyc-01", "cluster-sa-oeste-01"],
        casos_invalidos=[
            "cluster-01",
            "CLUSTER-us-east-01",
            "cluster-US-east-01",
            "cluster-us-east-1",
            "cluster-us-east-012",
            "cluster-us.east-01",
        ],
    )

    ok = ok1 + ok2
    total = total1 + total2
    print(f"RESULTADO FINAL: {ok}/{total} casos correctos.")
    # Código de salida útil para CI: 0 si todo pasa, 1 si hubo alguna falla.
    sys.exit(0 if ok == total else 1)
