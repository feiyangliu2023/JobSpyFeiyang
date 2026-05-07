"""Location string → (country_code, region) classifier.

SimplifyJobs/New-Grad-Positions and Summer2026-Internships use free-form
`locations[]` strings ("London, UK" / "Cambridge, MA" / "Bristol, UK" /
"Remote in EMEA"). To route them into our EMEA / North America / Other
buckets we need a small but careful classifier — careful because some city
names collide (Cambridge UK vs Cambridge MA, Birmingham UK vs Birmingham AL).

Strategy: prefer the rightmost comma-separated token as a country / state
hint, fall back to a known-city set only when no country suffix is given.
This avoids the `Birmingham, AL` → EMEA false positive that a naive
city-substring match would hit.
"""

from __future__ import annotations

# Region a country code rolls up into.
_REGION_MAP = {
    "us": "north_america",
    "ca": "north_america",
    "uk": "emea",
    "ie": "emea",
    "de": "emea",
    "fr": "emea",
    "nl": "emea",
    "ch": "emea",
    "se": "emea",
    "no": "emea",
    "dk": "emea",
    "fi": "emea",
    "es": "emea",
    "it": "emea",
    "pl": "emea",
    "cz": "emea",
    "be": "emea",
    "at": "emea",
    "pt": "emea",
    "il": "emea",
    "gr": "emea",
    "ro": "emea",
    "hu": "emea",
    "tr": "emea",
    "ae": "emea",
    "lu": "emea",
    "ee": "emea",
    "lt": "emea",
    "lv": "emea",
}

# 2-letter US postal state codes — used to disambiguate "Cambridge, MA"
# from "Cambridge, UK". Includes DC and territory PR.
_US_STATES = frozenset({
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id",
    "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms",
    "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
    "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv",
    "wi", "wy", "dc", "pr",
})

# Canadian province codes.
_CA_PROVINCES = frozenset({
    "ab", "bc", "mb", "nb", "nl", "ns", "nt", "nu", "on", "pe", "qc", "sk",
    "yt",
})

# Country / region word forms → 2-letter code. Includes a few colloquial
# variants we've seen in real listings.
_COUNTRY_NAMES = {
    "usa": "us", "u.s.a.": "us", "united states": "us", "u.s.": "us",
    "us": "us", "america": "us",
    "canada": "ca",
    "united kingdom": "uk", "uk": "uk", "u.k.": "uk", "england": "uk",
    "scotland": "uk", "wales": "uk", "northern ireland": "uk",
    "great britain": "uk", "britain": "uk",
    "germany": "de", "deutschland": "de",
    "ireland": "ie", "republic of ireland": "ie",
    "france": "fr",
    "netherlands": "nl", "the netherlands": "nl", "holland": "nl",
    "switzerland": "ch", "schweiz": "ch", "suisse": "ch",
    "sweden": "se", "sverige": "se",
    "norway": "no", "norge": "no",
    "denmark": "dk", "danmark": "dk",
    "finland": "fi", "suomi": "fi",
    "spain": "es", "españa": "es", "espana": "es",
    "italy": "it", "italia": "it",
    "poland": "pl", "polska": "pl",
    "czech republic": "cz", "czechia": "cz",
    "belgium": "be", "belgique": "be",
    "austria": "at", "österreich": "at",
    "portugal": "pt",
    "israel": "il",
    "greece": "gr",
    "romania": "ro",
    "hungary": "hu",
    "turkey": "tr",
    "uae": "ae", "united arab emirates": "ae",
    "luxembourg": "lu",
    "estonia": "ee", "lithuania": "lt", "latvia": "lv",
    # Non-EMEA regions we recognize but route to "other"
    "india": "in",
    "singapore": "sg",
    "hong kong": "hk",
    "china": "cn",
    "japan": "jp",
    "south korea": "kr", "korea": "kr",
    "australia": "au",
    "new zealand": "nz",
    "brazil": "br",
    "mexico": "mx",
    "argentina": "ar",
    "chile": "cl",
}

# Cities whose names alone (no country suffix in the listing string) are
# unambiguous EMEA. We deliberately avoid ambiguous names like Birmingham
# (AL/UK), Cambridge (MA/UK), Manchester (NH/UK) — those need a country
# suffix to classify.
_UNAMBIGUOUS_EMEA_CITIES = frozenset({
    "london", "edinburgh", "glasgow", "bristol", "leeds", "sheffield",
    "liverpool", "nottingham", "newcastle upon tyne", "newcastle",
    "reading", "oxford", "crawley", "milton keynes",
    "dublin", "cork", "galway",
    "berlin", "munich", "münchen", "hamburg", "frankfurt", "stuttgart",
    "köln", "cologne", "düsseldorf", "leipzig", "bremen", "hanover",
    "paris", "lyon", "toulouse", "lille", "marseille", "nantes",
    "amsterdam", "rotterdam", "eindhoven", "the hague", "den haag",
    "utrecht",
    "zürich", "zurich", "geneva", "genève", "basel", "lausanne",
    "stockholm", "gothenburg", "göteborg", "malmö", "uppsala",
    "oslo", "bergen", "trondheim",
    "copenhagen", "københavn", "aarhus",
    "helsinki", "tampere", "espoo",
    "madrid", "barcelona", "valencia", "seville", "zaragoza",
    "milan", "milano", "rome", "roma", "turin", "torino", "florence",
    "warsaw", "warszawa", "krakow", "kraków", "gdansk", "wrocław",
    "prague", "praha", "brno",
    "vienna", "wien", "graz", "salzburg",
    "brussels", "bruxelles", "antwerp", "ghent",
    "lisbon", "lisboa", "porto",
    "tel aviv", "haifa", "jerusalem",
    "athens", "thessaloniki",
    "budapest",
    "luxembourg city",
    "bucharest", "cluj-napoca",
    "tallinn", "vilnius", "riga",
})


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def classify_location(loc: str) -> tuple[str, str]:
    """Classify one location string → (country_code, region).

    Returns ("", "other") when nothing matches. The classifier is
    suffix-biased: it trusts the rightmost comma-separated token before
    looking at city names, because "Birmingham, AL" should never become
    EMEA even though Birmingham UK is well-known.
    """
    s = _norm(loc)
    if not s:
        return ("", "other")

    # "Remote" first — these don't carry a region by themselves.
    if "remote" in s and "," not in s:
        return ("remote", "other")

    parts = [p.strip() for p in s.split(",")]
    suffix = parts[-1] if parts else ""

    # Country / region in the suffix takes priority.
    if suffix in _COUNTRY_NAMES:
        cc = _COUNTRY_NAMES[suffix]
        return (cc, _REGION_MAP.get(cc, "other"))
    if suffix in _US_STATES:
        return ("us", "north_america")
    if suffix in _CA_PROVINCES:
        return ("ca", "north_america")

    # Whole string match (handles "United Kingdom" with no further detail).
    if s in _COUNTRY_NAMES:
        cc = _COUNTRY_NAMES[s]
        return (cc, _REGION_MAP.get(cc, "other"))

    # Fall back to unambiguous-city set, using the LEFTMOST token (the city
    # name comes first in "City, Country" formatting).
    first = parts[0] if parts else s
    if first in _UNAMBIGUOUS_EMEA_CITIES:
        return ("emea_city", "emea")

    # Some EU listings use "City, Country" but with the country name
    # spelled out. Already handled above. If we got here, give up.
    return ("", "other")


def classify_locations(locations) -> tuple[str, str]:
    """Best (country, region) for a list of locations.

    Preference order: EMEA → North America → Other. This bias is
    deliberate — when a posting lists "London, UK · New York, NY" it's
    primarily an EMEA opportunity (US listing acts as a fallback site)
    and we want it in the EMEA section.
    """
    if not locations:
        return ("", "other")
    seen = []
    for loc in locations:
        seen.append(classify_location(str(loc)))
    for cc, region in seen:
        if region == "emea":
            return (cc, region)
    for cc, region in seen:
        if region == "north_america":
            return (cc, region)
    return seen[0]
