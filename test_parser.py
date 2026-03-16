import re
import json

QUALITY_PATTERN = re.compile(r"\b(480p|720p|1080p|2160p|4k)\b", re.IGNORECASE)
YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")

# Original
# LANG_TOKEN_PATTERN = re.compile(
#     r"\b(dual(?:\s*audio)?|multi(?:\s*audio)?|hindi|english|tamil|telugu|malayalam|kannada|bengali|punjabi|marathi)\b",
#     re.IGNORECASE,
# )

# Updated Language Pattern (handles nf_series_dual without \b issue)
LANG_TOKEN_PATTERN = re.compile(
    r"(dual(?:\s*audio)?|multi(?:\s*audio)?|hindi|english|tamil|telugu|malayalam|kannada|bengali|punjabi|marathi)",
    re.IGNORECASE,
)

# Expand SEASON/EP patterns:
# S01, S1, Season 1, S01_To_05, E01, Ep 1-9
SEASON_RANGE_PATTERN = re.compile(
    r"\b(?:s(?:eason)?\s*0?(\d{1,2})\s*(?:to|\-|_)\s*0?(\d{1,2})|s0?(\d{1,2})\s*(?:to|\-|_)\s*0?(\d{1,2}))\b",
    re.IGNORECASE,
)
SEASON_EP_PATTERN = re.compile(r"\bs(?:eason)?\s*0?(\d{1,2})\s*e(?:p(?:isode)?)?\s*0?(\d{1,3})\b", re.IGNORECASE)
SEASON_SINGLE_PATTERN = re.compile(r"\b(?:season\s*0?(\d{1,2})|s\s*0?(\d{1,2}))\b", re.IGNORECASE)

# Note Ep 1-9
EPISODE_RANGE_PATTERN = re.compile(r"\be(?:p(?:isode)?)?\s*0?(\d{1,3})\s*(?:to|\-|_)\s*0?(\d{1,3})\b", re.IGNORECASE)
EPISODE_SINGLE_PATTERN = re.compile(r"\be(?:p(?:isode)?)?\s*0?(\d{1,3})\b", re.IGNORECASE)

NOISE_PATTERN = re.compile(
    r"(@[a-zA-Z0-9_]+|mkv|mp4|avi|x264|x265|hevc|hdrip|web-?dl|webrip|bluray|aac|10bit|esub)",
    re.IGNORECASE,
)


def _normalize_lang_token(token: str) -> list[str]:
    t = re.sub(r"\s+", " ", (token or "").strip().lower())
    if t in {"dual", "dual audio"}:
        return ["hindi", "english"]
    if t in {"multi", "multi audio"}:
        return ["multi"]
    return [t] if t else []

def _extract_season_and_ep(normalized_text: str) -> str:
    # returns like S1, S1-S5, E1, E1-E9, S1 E2
    
    season = ""
    ep = ""
    
    # Season + Ep match
    se_match = SEASON_EP_PATTERN.search(normalized_text)
    if se_match:
        return f"S{int(se_match.group(1))} E{int(se_match.group(2))}"
    
    # Season ranges
    range_match = SEASON_RANGE_PATTERN.search(normalized_text)
    if range_match:
        start = int(range_match.group(1) or range_match.group(3))
        end = int(range_match.group(2) or range_match.group(4))
        start, end = min(start, end), max(start, end)
        if start == end:
            return f"S{start}"
        return f"S{start}-S{end}"
        
    single_match = SEASON_SINGLE_PATTERN.search(normalized_text)
    if single_match:
        season = f"S{int(single_match.group(1) or single_match.group(2))}"
    
    # Episodes ranges
    ep_range_match = EPISODE_RANGE_PATTERN.search(normalized_text)
    if ep_range_match:
        start = int(ep_range_match.group(1))
        end = int(ep_range_match.group(2))
        start, end = min(start, end), max(start, end)
        if start == end:
            ep = f"E{start}"
        else:
            ep = f"E{start}-E{end}"
    else:
        ep_single_match = EPISODE_SINGLE_PATTERN.search(normalized_text)
        if ep_single_match:
            ep = f"E{int(ep_single_match.group(1))}"
            
    if season and ep: return f"{season} {ep}"
    if season: return season
    if ep: return ep
    return ""


def parse_media_metadata(raw_text: str) -> dict:
    text = (raw_text or "").strip()
    normalized = re.sub(r"[._]", " ", text)
    
    quality_match = QUALITY_PATTERN.search(normalized)
    quality = quality_match.group(1).lower() if quality_match else "unknown"
    if quality == "4k":
        quality = "2160p"

    season = _extract_season_and_ep(normalized)

    langs = []
    # use word boundaries to avoid matching "hindi" in "thindik" or "english" inside something?
    # but handle NF_Series_Dual
    # the issue with \b was _ and . were replaced with spaces so NF Series Dual became separate words.
    # WAIT! If they were substituted with spaces, then "NF Series Dual" HAS WORD BOUNDARIES!!!
    # Why did it fail? NF Series Dual -> \bdual\b -> it should have matched!
    # Ah, the original code had \b(dual(?:\s*audio)?|multi(?:\s*audio)?|...)\b. IF the user had NF_Series_Dual_Audio, 
    # normalized = "NF Series Dual Audio". \bdual audio\b matches? Yes.
    # Wait, `NF_Series_Dual_Audio` -> `NF Series Dual Audio` -> `dual audio` match!
    # Why did it fail then? Let me test!
    
    for match in LANG_TOKEN_PATTERN.findall(normalized):
        for item in _normalize_lang_token(match):
            if item and item not in langs:
                langs.append(item)

    year_match = YEAR_PATTERN.search(normalized)
    year = int(year_match.group(1)) if year_match else 0

    clean_title = text
    clean_title = YEAR_PATTERN.sub(" ", clean_title)
    
    # strip language
    for mat in LANG_TOKEN_PATTERN.finditer(clean_title):
        clean_title = clean_title.replace(mat.group(0), " ")
    
    clean_title = NOISE_PATTERN.sub(" ", clean_title)
    
    # remove seasons and episodes from clean_title
    clean_title = SEASON_RANGE_PATTERN.sub(" ", clean_title)
    clean_title = SEASON_EP_PATTERN.sub(" ", clean_title)
    clean_title = SEASON_SINGLE_PATTERN.sub(" ", clean_title)
    clean_title = EPISODE_RANGE_PATTERN.sub(" ", clean_title)
    clean_title = EPISODE_SINGLE_PATTERN.sub(" ", clean_title)
    
    clean_title = re.sub(r"[._\[\]\(\)\-]+", " ", clean_title)
    clean_title = re.sub(r"\s+", " ", clean_title).strip().lower()

    return {
        "quality": quality,
        "languages": langs,
        "language": " ".join(langs) if langs else "unknown",
        "season": season,
        "year": year,
        "clean_title": clean_title,
    }


test_cases = [
    "Money_Heist_S01_NF_Series_Dual_Audio_720p",
    "Money_Heist_Season_1_NF_Series_Dual_Audio_1080p",
    "Game_of_Thrones_S01_To_05_1080p_Multi",
    "Show_S1-S5_720p_Hindi",
    "Another_Show_Ep_1-9_1080p_English",
    "Third_Show_E01_Tamil_480p",
    "Matrix_1999_1080p_Dual_Audio.mkv",
    "My_Series_S02E01_1080p_esub",
    "[www.Example.com] - The.Flash.S01E03.720p.HDTV.mkv"
]

for t in test_cases:
    print(f"Original: {t}")
    res = parse_media_metadata(t)
    print(f"Season: '{res['season']}'")
    print(f"Langs: '{res['language']}'")
    print(f"Clean: '{res['clean_title']}'")
    print("-" * 40)
