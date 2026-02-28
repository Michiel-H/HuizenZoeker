"""Registry of all available collectors."""

from __future__ import annotations

from src.collectors.base import BaseCollector
from src.collectors.pararius import ParariusCollector
from src.collectors.funda import FundaCollector
from src.collectors.huurwoningen import HuurwoningenCollector
from src.collectors.kamernet import KamernetCollector
from src.collectors.wonen123 import Wonen123Collector
from src.collectors.vesteda import VestedaCollector
from src.collectors.hausing import HausingCollector
from src.collectors.rental_agency import RentalAgencyCollector
from src.collectors.verhuurmakelaar_bas import VerhuurmakelaarBasCollector
from src.collectors.amsterdam_wonen import AmsterdamWonenCollector
from src.collectors.vbt import VBTCollector
from src.collectors.puur_makelaars import PuurMakelaarsCollector
from src.collectors.househunting import HouseHuntingCollector
from src.collectors.amsterdam_rentals import AmsterdamRentalsCollector
from src.collectors.inter_immo import InterImmoCollector
from src.collectors.vva_amsterdam import VVAAmsterdamCollector
from src.collectors.hestiva import HestivaCollector


ALL_COLLECTORS: list[type[BaseCollector]] = [
    ParariusCollector,
    FundaCollector,
    HuurwoningenCollector,
    KamernetCollector,
    Wonen123Collector,
    VestedaCollector,
    HausingCollector,
    RentalAgencyCollector,
    VerhuurmakelaarBasCollector,
    AmsterdamWonenCollector,
    VBTCollector,
    PuurMakelaarsCollector,
    HouseHuntingCollector,
    AmsterdamRentalsCollector,
    InterImmoCollector,
    VVAAmsterdamCollector,
    HestivaCollector,
]


def get_all_collectors() -> list[BaseCollector]:
    """Instantiate all registered collectors."""
    return [cls() for cls in ALL_COLLECTORS]
