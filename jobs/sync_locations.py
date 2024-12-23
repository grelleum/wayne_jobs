"""Sync Locations From CSV Job.

2024 Greg Mueller.
"""

import csv
from dataclasses import dataclass
from itertools import chain
from io import StringIO
from typing import Optional

from nautobot.apps.jobs import FileVar, Job, register_jobs
from nautobot.dcim.models import Location, LocationType
from nautobot.extras.models import Status

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


@dataclass
class LocationRecord:
    name: str
    location_type__name: str
    parent__name: Optional[str]
    parent__location_type__name: Optional[str]


class LocationsCSVImportJob(Job):
    """Job that imports Location data from a CSV file."""

    source_file = FileVar(description="CSV file containing Locations data")

    class Meta:
        """Metaclass attributes of LocationsCSVImportJob."""

        name = "Import locations from CSV file."
        description = "Job that keeps the Locations table up to date."

    def run(self, source_file):
        """Execute job logic."""
        self.source_file = source_file
        csv_records = self.get_csv_records(self.source_file)
        csv_records = self.translate_state_names(csv_records)
        location_records = self.iter_all_location_records(csv_records)
        self.process_source_records(location_records)

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
            "name": record["name"],
            "city": record["city"],
            "state": state_name,
        }

    def iter_all_location_records(self, records):
        yield from chain(
            self.get_states(records),
            self.get_cities(records),
            self.get_location_sites(records),
        )

    def get_states(self, records):
        state_names = set(r["state"] for r in records)
        state_records = [
            LocationRecord(
                name=state_name,
                location_type__name="State",
                parent__name=None,
                parent__location_type__name=None,
            )
            for state_name in state_names
        ]
        return state_records

    def get_cities(self, records):
        cities = set((r["city"], r["state"]) for r in records)
        city_records = [
            LocationRecord(
                name=city_name,
                location_type__name="City",
                parent__name=state_name,
                parent__location_type__name="State",
            )
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
            record = LocationRecord(
                name=site_name,
                location_type__name=location_type__name,
                parent__name=city_name,
                parent__location_type__name="City",
            )
            site_records.append(record)
        return site_records

    def process_source_records(self, location_records):
        existing_locations = self.get_existing_locations()
        active_status = Status.objects.get(name="Active")
        for record in location_records:
            identifiers = record.name, record.location_type__name
            if identifiers not in existing_locations:
                self.create_new_location(record, active_status)
                continue

            existing_location_attributes = existing_locations[identifiers]
            record_attributes = (
                record.parent__name,
                record.parent__location_type__name,
                "Active",
            )
            if record_attributes != existing_location_attributes:
                self.update_existing_location(record, active_status)

        # This code can be used to clean up locations that are not in the source file.
        # Alternatively, we could just mark them as status="Decommissioned".
        self.delete_missing_locations(location_records, existing_locations)

    def get_existing_locations(self):
        locations = Location.objects.values_list(
            "name",
            "location_type__name",
            "parent__name",
            "parent__location_type__name",
            "status__name",
        )
        return {
            (name, location_type): (parent, parent__location_type, status__name)
            for name, location_type, parent, parent__location_type, status__name in locations
        }

    def create_new_location(self, record: LocationRecord, status: Status):
        """Create a Location model instance when none exists."""
        location_type = self.get_location_type(record)
        parent = self.get_parent(record)
        obj = Location.objects.create(
            name=record.name,
            location_type=location_type,
            parent=parent,
            status=status,
        )
        self.logger.info(f"Created a new record for {obj}", extra={"object": obj})
        return obj

    def update_existing_location(self, record: LocationRecord, status: Status):
        """Create a Location model instance when none exists."""
        location_type = self.get_location_type(record)
        parent = self.get_parent(record)
        obj = Location.objects.get(name=record.name, location_type=location_type)
        obj.parent = parent
        obj.status = status
        obj.validated_save()
        self.logger.info(f"Updated location record for {obj}", extra={"object": obj})
        return obj

    def get_parent(self, record):
        """Return a Parent Location object matching the record's parent attributes."""
        if record.parent__name and record.parent__location_type__name:
            results = Location.objects.filter(
                name=record.parent__name,
                location_type__name=record.parent__location_type__name,
            )
            return results.first()

    def get_location_type(self, record):
        """Return a LocationType object matching the record.location_type__name."""
        results = LocationType.objects.filter(name=record.location_type__name)
        return results.first()

    def delete_missing_locations(self, location_records, existing_locations):
        """Remove existing locations that are not in the source file.

        Alternatively, we could just mark them as status="Decommissioned".
        """
        self.logger.debug("Location Delete is Not Implemented")


jobs = [LocationsCSVImportJob]
register_jobs(*jobs)
