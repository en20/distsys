"""Fontes de dados do domínio Agricultura de precisão (Pessoa A).

Cada fonte roda como um processo separado:

    python -m sources.valve            # válvula de irrigação (controlável)
    python -m sources.reservoir        # nível do reservatório (controlável, limiar)
    python -m sources.soil_moisture    # umidade do solo (contínua)
    python -m sources.weather_station  # estação meteorológica (contínua)
"""
