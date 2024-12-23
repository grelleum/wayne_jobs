"""Sync Locations From CSV Job.

2024 Greg Mueller.
"""


# import sys
# import time

# from django.conf import settings
# from django.db import transaction

# from nautobot.apps.jobs import (
#     DryRunVar,
    
#     IntegerVar,
#     Job,
#     JobButtonReceiver,
#     JobHookReceiver,
#     JSONVar,
#     register_jobs,
#     StringVar,
# )

from nautobot.extras.jobs import FileVar, Job


from nautobot.dcim.models import Device, Location, LocationType
# from nautobot.extras.choices import ObjectChangeActionChoices

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


class ImportLocationsFromCSVJob(Job):
    """Job that imports Location data from a CSV file."""

    input_file = FileVar(description="CSV file containing Locations data")

    class Meta:
        name = "Import locations from CSV file."
        description = "Job that keeps the Locations table up to date."

    def run(self, *args, **kwargs):
        self.logger.info(f'{repr(args)=}')
        self.logger.info(f'{repr(kwargs)=}')
