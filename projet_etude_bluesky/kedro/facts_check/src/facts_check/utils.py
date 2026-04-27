import functools
import logging
from codecarbon import EmissionsTracker
import os

log = logging.getLogger(__name__)

def track_emissions(project_name="Thumalien"):
    """
    Décorateur pour suivre la consommation énergétique d'une fonction Kedro.
    Les résultats sont sauvegardés dans kedro/facts_check/data/08_reporting/emissions.csv
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Chemin absolu dans le container pour éviter les doublons de dossiers
            output_dir = "/opt/airflow/kedro/facts_check/data/08_reporting"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            # Initialiser le tracker
            tracker = EmissionsTracker(
                project_name=f"{project_name} - {func.__name__}",
                output_dir=output_dir,
                output_file="emissions.csv",
                measure_power_secs=10,
                log_level="error",
                tracking_mode="process" # Plus adapté au CPU dans un container Docker
            )
            
            tracker.start()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                emissions_data = tracker.stop()
                log.info(f"[GreenIT] Empreinte carbone pour {func.__name__}: {emissions_data} kg CO2")
        return wrapper
    return decorator
