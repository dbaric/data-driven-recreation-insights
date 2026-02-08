"""
Run pipelines in order: people -> faculties -> map_faculties_to_people -> events -> reservations
"""

import os

os.environ["PEOPLE_PIPELINE_SKIP_GEOCODE"] = "0"

from pipelines.people.job import main as people_main
from pipelines.faculties.job import main as faculties_main
from pipelines.map_faculties_to_people.job import main as map_faculties_main
from pipelines.events.job import main as events_main
from pipelines.reservations.job import main as reservations_main

if __name__ == "__main__":
    people_main()
    faculties_main()
    map_faculties_main()
    events_main()
    reservations_main()
