"""Sync Locations From CSV Job.

2024 Greg Mueller.
"""

import csv
from io import StringIO
from typing import Optional

from django.urls import reverse
from diffsync import Adapter
from nautobot.apps.jobs import FileVar, register_jobs
from nautobot.dcim.models import Location
from nautobot_ssot.contrib import NautobotAdapter, NautobotModel
from nautobot_ssot.jobs.base import DataMapping, DataSource


name = "Wayne Enterprises: Custom Jobs"


STATE_ABBREVIATION_TO_FULL_NAME_MAP = {
    "AL": "Alabama",
    "KY": "Kentucky",
    "OH": "Ohio",
    "AK": "Alaska",
    "LA": "Louisiana",
    "OK": "Oklahoma",
    "AZ": "Arizona",
    "ME": "Maine",
    "OR": "Oregon",
    "AR": "Arkansas",
    "MD": "Maryland",
    "PA": "Pennsylvania",
    "AS": "American Samoa",
    "MA": "Massachusetts",
    "PR": "Puerto Rico",
    "CA": "California",
    "MI": "Michigan",
    "RI": "Rhode Island",
    "CO": "Colorado",
    "MN": "Minnesota",
    "SC": "South Carolina",
    "CT": "Connecticut",
    "MS": "Mississippi",
    "SD": "South Dakota",
    "DE": "Delaware",
    "MO": "Missouri",
    "TN": "Tennessee",
    "DC": "District of Columbia",
    "MT": "Montana",
    "TX": "Texas",
    "FL": "Florida",
    "NE": "Nebraska",
    "TT": "Trust Territories",
    "GA": "Georgia",
    "NV": "Nevada",
    "UT": "Utah",
    "GU": "Guam",
    "NH": "New Hampshire",
    "VT": "Vermont",
    "HI": "Hawaii",
    "NJ": "New Jersey",
    "VA": "Virginia",
    "ID": "Idaho",
    "NM": "New Mexico",
    "VI": "Virgin Islands",
    "IL": "Illinois",
    "NY": "New York",
    "WA": "Washington",
    "IN": "Indiana",
    "NC": "North Carolina",
    "WV": "West Virginia",
    "IA": "Iowa",
    "ND": "North Dakota",
    "WI": "Wisconsin",
    "KS": "Kansas",
    "MP": "Northern Mariana Islands",
    "WY": "Wyoming",
}


class LocationModel(NautobotModel):
    """Shared data model representing a Location in either of the local or remote Nautobot instances."""

    _model = Location
    _modelname = "location"
    _identifiers = ("name",)
    _attributes = (
        "location_type__name",
        "status__name",
        "parent__name",
        "parent__location_type__name",
        "description",
    )

    name: str
    location_type__name: str
    status__name: str
    parent__name: Optional[str] = None
    parent__location_type__name: Optional[str] = None
    description: str


class NautobotLocal(NautobotAdapter):
    """DiffSync adapter class for loading data from the local Nautobot instance."""

    location = LocationModel
    top_level = ["location"]


class NautobotRemote(Adapter):
    """DiffSync adapter class for loading data from a CSV File."""

    location = LocationModel
    top_level = ["location"]

    def __init__(self, *args, source_file, job=None, **kwargs):
        """Instantiate this class."""
        super().__init__(*args, **kwargs)
        self.job = job
        self.source_file = source_file

    def load(self):
        """Load data from the provided."""
        csv_records = self.get_csv_records(self.source_file)
        csv_records = self.translate_state_names(csv_records)
        location_records = self.get_all_location_records(csv_records)
        self.load_locations(location_records)

    def load_locations(self, location_records):
        """Load Locations data from the remote Nautobot instance."""
        for location_record in location_records:
            self.job.logger.debug(f"Location record: {location_record}")    # nocommit
            location = self.location(**location_record)
            self.add(location)
            self.job.logger.debug(
                f"Loaded {location} Location from remote Nautobot instance"
            )

    def get_csv_records(self, source_file):
        text = source_file.read().decode("utf-8")
        csv_data = csv.DictReader(StringIO(text))
        records = list(csv_data)
        return records

    def translate_state_names(self, csv_records):
        return [self.fix_state_name_in_source_record(r) for r in csv_records]

    def fix_state_name_in_source_record(self, record):
        state_name = record["state"]
        state_name = STATE_ABBREVIATION_TO_FULL_NAME_MAP.get(state_name, state_name)
        return {
            'name': record["name"],
            'city': record["city"],
            'state': state_name,
        }

    def get_all_location_records(self, records):
        return [
            *self.get_states(records),
            *self.get_cities(records),
            *self.get_location_sites(records),
        ]

    def get_states(self, records):
        state_names = set(r["state"] for r in records)
        state_records = [
            {
                "name": state_name,
                "location_type__name": "State",
                "status__name": "Active",
                "description": f"The state of '{state_name}'.",
            }
            for state_name in state_names
        ]
        return state_records

    def get_cities(self, records):
        cities = set((r["city"], r["state"]) for r in records)
        city_records = [
            {
                "name": city_name,
                "location_type__name": "City",
                "status__name": "Active",
                "parent__name": state_name,
                "parent__location_type__name": "State",
                "description": f"The city of '{city_name}'.",
            }
            for city_name, state_name in cities
        ]
        return city_records

    def get_location_sites(self, records):
        site_records = []
        for site in records:
            site_name = site["name"]
            city_name = site["city"]
            if site_name.endswith("-BR"):
                location_type__name = "Branch"
            elif site_name.endswith("-DC"):
                location_type__name = "Data Center"
            else:
                self.logger.warn(f"'{site_name}' is not a Branch or Data Center")
                continue
            record = {
                "name": site_name,
                "location_type__name": location_type__name,
                "status__name": "Active",
                "parent__name": city_name,
                "parent__location_type__name": "City",
                "description": f"The {location_type__name} of '{site_name}'.",
            }
            site_records.append(record)

        return site_records


class LocationsCSVDataSource(DataSource):
    """Job that imports Location data from a CSV file."""

    source_file = FileVar(description="CSV file containing Locations data")

    class Meta:
        """Metaclass attributes of LocationsCSVDataSource."""

        name = "Import locations from CSV file."
        description = "Job that keeps the Locations table up to date."
        data_source = "Locations CSV File (remote)"

    @classmethod
    def data_mappings(cls):
        """Map remote source data to local Nautobot models."""
        return (
            DataMapping(
                "Location (remote)",
                None,
                "Location (local)",
                reverse("dcim:location_list"),
            ),
        )

    def run(self, *args, dryrun, memory_profiling, source_file, **kwargs):
        self.source_file = source_file
        self.dryrun = dryrun
        self.memory_profiling = memory_profiling
        super().run(dryrun, memory_profiling, *args, **kwargs)

    def load_source_adapter(self):
        """Method to instantiate and load the SOURCE adapter into `self.source_adapter`."""
        self.source_adapter = NautobotRemote(source_file=self.source_file, job=self)
        self.source_adapter.load()

    def load_target_adapter(self):
        """Method to instantiate and load the TARGET adapter into `self.target_adapter`."""
        self.target_adapter = NautobotLocal(job=self, sync=self.sync)
        self.target_adapter.load()


jobs = [LocationsCSVDataSource]
register_jobs(*jobs)
