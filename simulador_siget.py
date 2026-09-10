from collections import deque

# ============================================================
# SIMULADOR DE PLANIFICADOR DE CPU - SIGET
# Algoritmos: FIFO y Round Robin
# Estados: Nuevo, Listo, En ejecucion, Bloqueado y Terminado
# ============================================================


# ============================================================
# PROCESOS DEL SIGET
# ============================================================

PROCESOS_BASE = [
    {
        "id": "P1",
        "nombre": "Alerta critica",
        "tiempo_irrupcion": 0,
        "prioridad_alerta": 1,
        "tamano_datos": 90,
        "tiempo_cpu": 5
    },
    {
        "id": "P2",
        "nombre": "Trafico rutinario",
        "tiempo_irrupcion": 1,
        "prioridad_alerta": 3,
        "tamano_datos": 40,
        "tiempo_cpu": 4
    },
    {
        "id": "P3",
        "nombre": "Congestion",
        "tiempo_irrupcion": 2,
        "prioridad_alerta": 2,
        "tamano_datos": 65,
        "tiempo_cpu": 3
    }
]


# ============================================================
# CREAR COPIA DE LOS PROCESOS
# ============================================================

def crear_procesos():
    procesos = []

    for proceso in PROCESOS_BASE:
        nuevo = proceso.copy()
        nuevo["estado"] = "Nuevo"
        nuevo["restante"] = proceso["tiempo_cpu"]
        nuevo["tiempo_respuesta"] = None
        nuevo["tiempo_finalizacion"] = None
        nuevo["bloqueado_una_vez"] = False
        procesos.append(nuevo)

    return procesos


# ============================================================
# REGISTRAR CAMBIOS DE ESTADO
# ============================================================

def registrar(eventos, tiempo, proceso, estado, detalle):

    if proceso is None:
        nombre = "-"
    else:
        nombre = proceso["id"] + " - " + proceso["nombre"]

    eventos.append(
        (tiempo, nombre, estado, detalle)
    )


# ============================================================
# MOSTRAR PROCESOS
# ============================================================

def mostrar_procesos():

    print("\n" + "=" * 90)
    print("                 PROCESOS DEL SIGET")
    print("=" * 90)

    print(
        f"{'ID':<6}"
        f"{'Proceso':<25}"
        f"{'Irrupcion':<14}"
        f"{'Prioridad':<12}"
        f"{'Datos MB':<12}"
        f"{'CPU':<8}"
    )

    print("-" * 90)

    for p in PROCESOS_BASE:

        print(
            f"{p['id']:<6}"
            f"{p['nombre']:<25}"
            f"{p['tiempo_irrupcion']:<14}"
            f"{p['prioridad_alerta']:<12}"
            f"{p['tamano_datos']:<12}"
            f"{p['tiempo_cpu']:<8}"
        )


# ============================================================
# ALGORITMO FIFO
# ============================================================

def fifo():

    procesos = crear_procesos()

    cola = deque()
    bloqueados = []

    eventos = []
    linea_tiempo = []

    tiempo = 0

    registrar(
        eventos,
        tiempo,
        None,
        "Inicio",
        "Planificacion FIFO"
    )

    # --------------------------------------------------------
    # FIX: registrar explicitamente el estado "Nuevo" de cada
    # proceso antes de que ingresen a la cola de listos, para
    # que el estado quede visible en el log de eventos y en
    # la verificacion final de los 5 estados.
    # --------------------------------------------------------

    for p in procesos:

        registrar(
            eventos,
            tiempo,
            p,
            "Nuevo",
            "Proceso creado, en espera de su tiempo de irrupcion"
        )

    # --------------------------------------------------------
    # CICLO PRINCIPAL
    # --------------------------------------------------------

    while True:

        # Procesos que llegan
        for p in procesos:

            if (
                p["estado"] == "Nuevo"
                and p["tiempo_irrupcion"] <= tiempo
            ):

                p["estado"] = "Listo"
                cola.append(p)

                registrar(
                    eventos,
                    tiempo,
                    p,
                    "Listo",
                    "Ingresa a la cola de procesos listos"
                )

        # Procesos que terminan su bloqueo
        for elemento in bloqueados[:]:

            p, fin_bloqueo = elemento

            if tiempo >= fin_bloqueo:

                p["estado"] = "Listo"
                cola.append(p)

                bloqueados.remove(elemento)

                registrar(
                    eventos,
                    tiempo,
                    p,
                    "Listo",
                    "Finaliza el bloqueo y vuelve a la cola"
                )

        # Si no hay procesos listos
        if not cola:

            futuros = []

            for p in procesos:

                if p["estado"] == "Nuevo":
                    futuros.append(p["tiempo_irrupcion"])

            for _, fin_bloqueo in bloqueados:
                futuros.append(fin_bloqueo)

            if not futuros:
                break

            tiempo = min(futuros)

            continue

        # FIFO toma el primer proceso de la cola
        p = cola.popleft()

        p["estado"] = "En ejecucion"

        if p["tiempo_respuesta"] is None:

            p["tiempo_respuesta"] = (
                tiempo - p["tiempo_irrupcion"]
            )

        registrar(
            eventos,
            tiempo,
            p,
            "En ejecucion",
            "FIFO selecciona el primer proceso de la cola"
        )

        # FIFO ejecuta hasta terminar
        # pero P1 tendrá una espera de entrada/salida
        if (
            p["id"] == "P1"
            and not p["bloqueado_una_vez"]
        ):

            ejecutar = 2

            linea_tiempo.append(
                (tiempo, tiempo + ejecutar, p["id"])
            )

            p["restante"] -= ejecutar
            tiempo += ejecutar

            p["bloqueado_una_vez"] = True
            p["estado"] = "Bloqueado"

            registrar(
                eventos,
                tiempo,
                p,
                "Bloqueado",
                "P1 espera nuevos datos de los sensores"
            )

            bloqueados.append(
                (p, tiempo + 1)
            )

            continue

        # Ejecutar el tiempo restante
        ejecutar = p["restante"]

        linea_tiempo.append(
            (tiempo, tiempo + ejecutar, p["id"])
        )

        tiempo += ejecutar
        p["restante"] = 0

        p["estado"] = "Terminado"
        p["tiempo_finalizacion"] = tiempo

        registrar(
            eventos,
            tiempo,
            p,
            "Terminado",
            "Finaliza completamente el proceso"
        )

    return eventos, linea_tiempo, procesos


# ============================================================
# ALGORITMO ROUND ROBIN
# ============================================================

def round_robin(quantum=2):

    procesos = crear_procesos()

    cola = deque()
    bloqueados = []

    eventos = []
    linea_tiempo = []

    tiempo = 0

    registrar(
        eventos,
        tiempo,
        None,
        "Inicio",
        f"Planificacion Round Robin con quantum de {quantum}"
    )

    # --------------------------------------------------------
    # FIX: registrar explicitamente el estado "Nuevo" de cada
    # proceso antes de que ingresen a la cola de listos, para
    # que el estado quede visible en el log de eventos y en
    # la verificacion final de los 5 estados.
    # --------------------------------------------------------

    for p in procesos:

        registrar(
            eventos,
            tiempo,
            p,
            "Nuevo",
            "Proceso creado, en espera de su tiempo de irrupcion"
        )

    # --------------------------------------------------------
    # CICLO PRINCIPAL
    # --------------------------------------------------------

    while True:

        # Procesos que llegan
        for p in procesos:

            if (
                p["estado"] == "Nuevo"
                and p["tiempo_irrupcion"] <= tiempo
            ):

                p["estado"] = "Listo"
                cola.append(p)

                registrar(
                    eventos,
                    tiempo,
                    p,
                    "Listo",
                    "Ingresa a la cola de procesos listos"
                )

        # Procesos que terminan su bloqueo
        for elemento in bloqueados[:]:

            p, fin_bloqueo = elemento

            if tiempo >= fin_bloqueo:

                p["estado"] = "Listo"
                cola.append(p)

                bloqueados.remove(elemento)

                registrar(
                    eventos,
                    tiempo,
                    p,
                    "Listo",
                    "Finaliza el bloqueo y vuelve a la cola"
                )

        # Si no hay procesos listos
        if not cola:

            futuros = []

            for p in procesos:

                if p["estado"] == "Nuevo":
                    futuros.append(p["tiempo_irrupcion"])

            for _, fin_bloqueo in bloqueados:
                futuros.append(fin_bloqueo)

            if not futuros:
                break

            tiempo = min(futuros)

            continue

        # Round Robin toma el primer proceso
        p = cola.popleft()

        p["estado"] = "En ejecucion"

        if p["tiempo_respuesta"] is None:

            p["tiempo_respuesta"] = (
                tiempo - p["tiempo_irrupcion"]
            )

        registrar(
            eventos,
            tiempo,
            p,
            "En ejecucion",
            "Round Robin asigna la CPU"
        )

        # Tiempo de ejecución
        ejecutar = min(
            quantum,
            p["restante"]
        )

        linea_tiempo.append(
            (tiempo, tiempo + ejecutar, p["id"])
        )

        p["restante"] -= ejecutar
        tiempo += ejecutar

        # ----------------------------------------------------
        # BLOQUEO ESPECIAL DE P1
        # ----------------------------------------------------

        if (
            p["id"] == "P1"
            and not p["bloqueado_una_vez"]
            and p["restante"] > 0
        ):

            p["bloqueado_una_vez"] = True
            p["estado"] = "Bloqueado"

            registrar(
                eventos,
                tiempo,
                p,
                "Bloqueado",
                "P1 espera nuevos datos de los sensores"
            )

            bloqueados.append(
                (p, tiempo + 1)
            )

        # ----------------------------------------------------
        # PROCESO TERMINADO
        # ----------------------------------------------------

        elif p["restante"] == 0:

            p["estado"] = "Terminado"
            p["tiempo_finalizacion"] = tiempo

            registrar(
                eventos,
                tiempo,
                p,
                "Terminado",
                "Finaliza completamente el proceso"
            )

        # ----------------------------------------------------
        # REGRESA A LISTO
        # ----------------------------------------------------

        else:

            p["estado"] = "Listo"
            cola.append(p)

            registrar(
                eventos,
                tiempo,
                p,
                "Listo",
                "Termina su quantum y vuelve a la cola"
            )

    return eventos, linea_tiempo, procesos


# ============================================================
# MOSTRAR EVENTOS
# ============================================================

def imprimir_eventos(titulo, eventos):

    print("\n" + "=" * 100)
    print(titulo)
    print("=" * 100)

    print(
        f"{'Tiempo':<9}"
        f"{'Proceso':<30}"
        f"{'Estado':<18}"
        f"Detalle"
    )

    print("-" * 100)

    for tiempo, proceso, estado, detalle in eventos:

        print(
            f"{tiempo:<9}"
            f"{proceso:<30}"
            f"{estado:<18}"
            f"{detalle}"
        )


# ============================================================
# MOSTRAR LINEA DE TIEMPO
# ============================================================

def imprimir_linea_tiempo(titulo, linea_tiempo):

    print("\n" + "=" * 70)
    print(titulo)
    print("=" * 70)

    for inicio, fin, proceso in linea_tiempo:

        print(
            f"Tiempo {inicio} - {fin}  ->  {proceso}  ->  CPU"
        )


# ============================================================
# VERIFICAR LOS CINCO ESTADOS
# ============================================================

def verificar_estados(eventos):

    estados_requeridos = [
        "Nuevo",
        "Listo",
        "En ejecucion",
        "Bloqueado",
        "Terminado"
    ]

    encontrados = {}

    for estado in estados_requeridos:
        encontrados[estado] = False

    for _, _, estado, _ in eventos:

        if estado in encontrados:
            encontrados[estado] = True

    return encontrados


# ============================================================
# MOSTRAR RESUMEN
# ============================================================

def mostrar_resumen(procesos_fifo, procesos_rr):

    print("\n" + "=" * 95)
    print("                    COMPARACION DE RESULTADOS")
    print("=" * 95)

    print(
        f"{'ID':<6}"
        f"{'Proceso':<25}"
        f"{'Respuesta FIFO':<18}"
        f"{'Respuesta RR':<18}"
        f"{'Fin FIFO':<12}"
        f"{'Fin RR':<10}"
    )

    print("-" * 95)

    for fifo_proceso in procesos_fifo:

        for rr_proceso in procesos_rr:

            if fifo_proceso["id"] == rr_proceso["id"]:

                print(
                    f"{fifo_proceso['id']:<6}"
                    f"{fifo_proceso['nombre']:<25}"
                    f"{fifo_proceso['tiempo_respuesta']:<18}"
                    f"{rr_proceso['tiempo_respuesta']:<18}"
                    f"{fifo_proceso['tiempo_finalizacion']:<12}"
                    f"{rr_proceso['tiempo_finalizacion']:<10}"
                )


# ============================================================
# CONCLUSIONES
# ============================================================

def mostrar_conclusiones():

    print("\n" + "=" * 90)
    print("                         CONCLUSIONES")
    print("=" * 90)

    print()
    print("1. FIFO atiende los procesos respetando el orden de llegada.")
    print()
    print("2. Round Robin distribuye la CPU mediante un quantum de 2")
    print("   unidades de tiempo.")
    print()
    print("3. Round Robin permite que los procesos tengan oportunidades")
    print("   de utilizar la CPU sin que uno monopolice el procesador.")
    print()
    print("4. El bloqueo de P1 representa una espera por nuevos datos")
    print("   provenientes de los sensores de trafico.")
    print()
    print("5. En el escenario simulado del SIGET, Round Robin presenta")
    print("   un mejor equilibrio para atender diferentes tareas.")
    print()
    print("6. Para una emergencia, reducir el tiempo de respuesta es")
    print("   importante para contribuir a una movilidad urbana mas fluida.")


# ============================================================
# EJECUCION PRINCIPAL
# ============================================================

print("\n")
print("=" * 90)
print("             SIMULADOR DEL PLANIFICADOR CPU - SIGET")
print("=" * 90)

mostrar_procesos()


# ============================================================
# EJECUTAR FIFO
# ============================================================

eventos_fifo, linea_fifo, procesos_fifo = fifo()

imprimir_eventos(
    "ALGORITMO 1: FIFO",
    eventos_fifo
)

imprimir_linea_tiempo(
    "LINEA DE TIEMPO - FIFO",
    linea_fifo
)


# ============================================================
# EJECUTAR ROUND ROBIN
# ============================================================

eventos_rr, linea_rr, procesos_rr = round_robin(
    quantum=2
)

imprimir_eventos(
    "ALGORITMO 2: ROUND ROBIN",
    eventos_rr
)

imprimir_linea_tiempo(
    "LINEA DE TIEMPO - ROUND ROBIN",
    linea_rr
)


# ============================================================
# COMPARACION
# ============================================================

mostrar_resumen(
    procesos_fifo,
    procesos_rr
)


# ============================================================
# VERIFICACION DE ESTADOS
# ============================================================

print("\n" + "=" * 90)
print("                    VERIFICACION DE ESTADOS")
print("=" * 90)

for nombre, eventos in [
    ("FIFO", eventos_fifo),
    ("ROUND ROBIN", eventos_rr)
]:

    print("\n" + nombre)

    estados = verificar_estados(eventos)

    for estado, correcto in estados.items():

        resultado = "OK" if correcto else "PENDIENTE"

        print(
            f"  {estado:<18}: {resultado}"
        )


# ============================================================
# CONCLUSIONES
# ============================================================

mostrar_conclusiones()


print("\n" + "=" * 90)
print("              SIMULACION FINALIZADA CORRECTAMENTE")
print("=" * 90)