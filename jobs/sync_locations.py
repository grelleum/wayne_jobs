"""Sync Locations From CSV Job.

2024 Greg Mueller.
"""


import csv
from io import StringIO
from typing import Generator, List, Optional

from django.urls import reverse

from nautobot.apps.jobs import FileVar, register_jobs
from nautobot.dcim.models import Location, LocationType




# import requests
# from diffsync import Adapter
# from diffsync.enum import DiffSyncFlags
# from diffsync.exceptions import ObjectNotFound
# from django.contrib.contenttypes.models import ContentType
# from django.templatetags.static import static
# from nautobot.dcim.models import Device, DeviceType, Interface, Location, LocationType, Manufacturer, Platform
# from nautobot.extras.choices import SecretsGroupAccessTypeChoices, SecretsGroupSecretTypeChoices
# from nautobot.extras.jobs import ObjectVar, StringVar
# from nautobot.extras.models import ExternalIntegration, Role, Status
# from nautobot.extras.secrets.exceptions import SecretError
# from nautobot.ipam.models import IPAddress, Namespace, Prefix
# from nautobot.tenancy.models import Tenant

from nautobot_ssot.contrib import NautobotAdapter, NautobotModel
from nautobot_ssot.jobs.base import DataMapping, DataSource
# from nautobot_ssot.tests.contrib_base_classes import ContentTypeDict



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
            DataMapping("Location (remote)", None, "Location (local)", reverse("dcim:location_list")),
        )

    def run(self, *args, source_file, **kwargs):
        self.logger.info(f"{repr(args)=}")
        self.logger.info(f"{repr(kwargs)=}")
        records = self.get_csv_data(source_file)
        self.logger.info(f"{repr(records)=}")
        source_data = self.get_source_data(records)
        self.logger.info(f"{repr(source_data)=}")

    def get_csv_data(self, source_file):
        text = source_file.read().decode("utf-8")
        csv_data = csv.DictReader(StringIO(text))
        records = list(csv_data)
        return records

    def get_source_data(self, records):
        return [
            *self.get_states(records),
            *self.get_cities(records),
            *self.get_location_sites(records),
        ]

    def get_states(self, records):
        states = set(r["state"] for r in records)
        state_records = [
            {
                "name": state_name,
                "location_type__name": "State",
                "status__name": "Active",
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


    # def run(  # pylint: disable=too-many-arguments, arguments-differ
    #     self,
    #     dryrun,
    #     memory_profiling,
    #     source,
    #     source_url,
    #     source_token,
    #     *args,
    #     **kwargs,
    # ):
    #     """Run sync."""
    #     self.dryrun = dryrun
    #     self.memory_profiling = memory_profiling
    #     try:
    #         if source:
    #             self.logger.info(f"Using external integration '{source}'")
    #             self.source_url = source.remote_url
    #             if not source.secrets_group:
    #                 self.logger.error(
    #                     "%s is missing a SecretsGroup. You must specify a SecretsGroup to synchronize with this Nautobot instance.",
    #                     source,
    #                 )
    #                 raise MissingSecretsGroupException(message="Missing SecretsGroup on specified ExternalIntegration.")
    #             secrets_group = source.secrets_group
    #             self.source_token = secrets_group.get_secret_value(
    #                 access_type=SecretsGroupAccessTypeChoices.TYPE_HTTP,
    #                 secret_type=SecretsGroupSecretTypeChoices.TYPE_TOKEN,
    #             )
    #         else:
    #             self.source_url = source_url
    #             self.source_token = source_token
    #     except SecretError as error:
    #         self.logger.error("Error setting up job: %s", error)
    #         raise

    #     super().run(dryrun, memory_profiling, *args, **kwargs)

    # def load_source_adapter(self):
    #     """Method to instantiate and load the SOURCE adapter into `self.source_adapter`."""
    #     self.source_adapter = NautobotRemote(url=self.source_url, token=self.source_token, job=self)
    #     self.source_adapter.load()

    # def load_target_adapter(self):
    #     """Method to instantiate and load the TARGET adapter into `self.target_adapter`."""
    #     self.target_adapter = NautobotLocal(job=self, sync=self.sync)
    #     self.target_adapter.load()

    # def lookup_object(self, model_name, unique_id):
    #     """Look up a Nautobot object based on the DiffSync model name and unique ID."""
    #     if model_name == "prefix":
    #         try:
    #             return Prefix.objects.get(
    #                 prefix=unique_id.split("__")[0], tenant__name=unique_id.split("__")[1] or None
    #             )
    #         except Prefix.DoesNotExist:
    #             pass
    #     elif model_name == "tenant":
    #         try:
    #             return Tenant.objects.get(name=unique_id)
    #         except Tenant.DoesNotExist:
    #             pass
    #     return None



jobs = [LocationsCSVDataSource]
register_jobs(*jobs)
