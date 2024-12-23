"""Sync Locations From CSV Job.

2024 Greg Mueller.
"""


import csv
from io import StringIO

from nautobot.apps.jobs import FileVar, Job, register_jobs
from nautobot.dcim.models import Location, LocationType




# import requests
# from diffsync import Adapter
# from diffsync.enum import DiffSyncFlags
# from diffsync.exceptions import ObjectNotFound
# from django.contrib.contenttypes.models import ContentType
# from django.templatetags.static import static
# from django.urls import reverse
# from nautobot.dcim.models import Device, DeviceType, Interface, Location, LocationType, Manufacturer, Platform
# from nautobot.extras.choices import SecretsGroupAccessTypeChoices, SecretsGroupSecretTypeChoices
# from nautobot.extras.jobs import ObjectVar, StringVar
# from nautobot.extras.models import ExternalIntegration, Role, Status
# from nautobot.extras.secrets.exceptions import SecretError
# from nautobot.ipam.models import IPAddress, Namespace, Prefix
# from nautobot.tenancy.models import Tenant

from nautobot_ssot.contrib import NautobotAdapter, NautobotModel
from nautobot_ssot.jobs.base import DataMapping, DataSource, DataTarget
from nautobot_ssot.tests.contrib_base_classes import ContentTypeDict



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


class ImportLocationsFromCSVJob(Job):
    """Job that imports Location data from a CSV file."""

    input_file = FileVar(description="CSV file containing Locations data")

    class Meta:
        name = "Import locations from CSV file."
        description = "Job that keeps the Locations table up to date."

    def run(self, input_file):
        records = self.get_csv_data(input_file)
        self.logger.info(f"{repr(records)=}")

    def get_csv_data(self, input_file):
        text = input_file.read().decode("utf-8")
        csv_data = csv.DictReader(StringIO(text))
        records = list(csv_data)
        return records

    def get_states(self, records):
        states = set(r["state"] for r in records)
        state_records = [
            {
                "name": state_name,
                "location_type__name": "State",
                "status__name": "Active",
                # "parent__name": None,
                # "parent__location_type__name",
                # "tenant__name",
                "description": f"The state of '{state_name}'.",
            }
            for state_name in states
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


jobs = [ImportLocationsFromCSVJob]
register_jobs(*jobs)
