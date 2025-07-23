import pandas as pd
import uuid
import asyncio
import nest_asyncio
import re
import json
import os
from datetime import datetime, timedelta
from pyppeteer import launch
from bs4 import BeautifulSoup

nest_asyncio.apply()

RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_DIR = f"event_json_files/run_{RUN_TIMESTAMP}"
os.makedirs(OUTPUT_DIR, exist_ok=True)

SUMMARY = {
    "total": 0,
    "success": 0,
    "failed": 0,
    "failed_links": []
}

ALL_PLUR_JSONS = []

def create_empty_event():
    return {
        'externalEventLink': '', 'eventId': str(uuid.uuid4()), 'plurEventLink': '',
        'eventName': '', 'eventDescription': '', 'eventDateAndTime': '', 'eventDuration': '',
        'venueName': '', 'locality': '', 'address': '', 'zipcode': '', 'lat': '', 'lon': '',
        'ticketAmount': '', 'guestlistInfo': '', 'ticketLink': '', 'supportedLanguages': '',
        'category': '', 'recurrence': '', 'attendeesCount': '', 'joinChatDetails': '',
        'policyAndConditions': '', 'frequentlyAskedQuestions': '', 'city': '', 'state': '',
        'geolocation': '', 'isRecurring': 'No', 'pattern': '', 'eventType': '', 'subCategory': '',
        'venueId': '', 'foodAvailable': '', 'smokingAllowed': '', 'wheelchairAccess': '',
        'parkingAvailable': '', 'supportAvailable': '', 'layout': '', 'petFriendly': False,
        'alcoholServed': '', 'minimumAge': '', 'ticketsAtVenue': '', 'prohibitedItemList': '',
        'washroomAvailable': '', 'onlineMenu': '', 'happyHours': '', 'danceFloorAvailable': '',
        'poolAvailable': '', 'isActivityAvailable': '', 'artists': '', 'highlightImages': '',
        'highlightVideos': '', 'galleryImages': '', 'galleryVideos': '', 'kidFriendly': False,
        'seatingArrangement': '', 'entryAllowedFor': '', 'tags': '',
        'Plur_json_format': '', 'jsonFilePath': ''
    }

def build_plur_json(event):
    def to_int(val): return int(val) if str(val).isdigit() else 0
    def to_float(val): return float(val) if re.match(r'^-?\d+(?:\.\d+)?$', str(val)) else None

    return {
        "eventName": event["eventName"],
        "eventDescription": event["eventDescription"],
        "eventDateAndTime": event["eventDateAndTime"],
        "eventDuration": to_int(event["eventDuration"]),
        "venue": {
            "venueName": event["venueName"],
            "locality": event["locality"],
            "address": event["address"],
            "city": event["city"],
            "state": event["state"],
            "zipcode": event["zipcode"],
            "geolocation": {
                "lat": to_float(event["lat"]),
                "lon": to_float(event["lon"]),
            },
            "layout": event.get("layout", "")
        },
        "highlightImageLinks": [event["highlightImages"]] if event["highlightImages"] else [],
        "galleryImageLinks": [event["highlightImages"]] if event["highlightImages"] else [],
        "ticketAmount": to_int(event["ticketAmount"]),
        "ticketLink": event["ticketLink"],
        "supportedLanguages": ["English"],
        "category": event["category"],
        "subCategory": event["subCategory"],
        "eventType": event["eventType"],
        "eventFeatures": {
            "foodAvailable": event["foodAvailable"] == "Yes",
            "smokingAllowed": event["smokingAllowed"] == "Yes",
            "wheelchairAccess": event["wheelchairAccess"] == "Yes",
            "parkingAvailable": event["parkingAvailable"] == "Yes",
            "supportAvailable": event["supportAvailable"] == "Yes",
            "petFriendly": event["petFriendly"],
            "alcoholServed": event["alcoholServed"] == "Yes",
            "minimumAge": to_int(event["minimumAge"]),
            "ticketsAtVenue": event["ticketsAtVenue"] == "Yes",
            "washroomAvailable": event["washroomAvailable"] == "Yes",
            "danceFloorAvailable": event["danceFloorAvailable"] == "Yes",
            "poolAvailable": event["poolAvailable"] == "Yes"
        },
        "artists": event["artists"].split(", ") if event["artists"] else [],
        "sharableEventOgImageLink": event["highlightImages"],
        "attendeesCount": to_int(event["attendeesCount"]),
        "likesCount": 0,
        "joinChatDetails": {
            "joinChatLink": "",
            "provider": "",
            "isEnabled": False
        },
        "policyAndConditions": event["policyAndConditions"].split("\n") if event["policyAndConditions"] else [],
        "frequentlyAskedQuestions": []
    }

async def scrape_highape_event(url):
    event = create_empty_event()
    event['externalEventLink'] = url
    try:
        browser = await launch(headless=False, args=['--no-sandbox'])
        page = await browser.newPage()
        await page.setUserAgent('Mozilla/5.0')
        await page.goto(url, {'waitUntil': 'domcontentloaded', 'timeout': 60000})
        await asyncio.sleep(5)
        raw_html = await page.content()
        await browser.close()

        soup = BeautifulSoup(raw_html, 'html.parser')
        event['rawHtml'] = raw_html

        def sel(selector): return soup.select_one(selector)
        def txt(selector): return sel(selector).get_text(strip=True) if sel(selector) else ''
        def href(selector): return sel(selector)['href'] if sel(selector) else ''

        event['eventName'] = txt('h1.mob-event-name-heading') or txt('h1.event-name-heading')
        venue_full = txt('h2.mob-venue-name-details-event') or txt('h2.venue-name-details-event')
        event['venueName'] = re.sub(r'^.*?-\s*', '', venue_full)
        event['eventDescription'] = txt('#desc .event-content-div')
        event['eventDateAndTime'] = txt('div.quick_look_divs:nth-child(2) span.details')
        event['ticketAmount'] = txt('div.quick_look_divs:nth-child(3) span.details') or 'Free Entry'
        event['ticketLink'] = href('a#book_thru_me')
        event['category'] = ', '.join([e.get_text(strip=True) for e in soup.select('div.quick_look_divs h4.category_text')])
        event['artists'] = ', '.join([e.get_text(strip=True) for e in soup.select('#artist h3')])
        event['highlightImages'] = sel('#image_carousel_web img.img-background-events')['src'] if sel('#image_carousel_web img.img-background-events') else ''

        address = txt('#venue .address p')
        event['address'] = address
        zip_match = re.search(r'\b(\d{6})\b', address)
        if zip_match: event['zipcode'] = zip_match.group(1)
        parts = re.findall(r'[A-Za-z]+', address)
        if len(parts) >= 4:
            event['locality'] = parts[-4]
            event['city'] = parts[-3]
            event['state'] = parts[-2]

        event['lat'] = sel('#venue_lat')['value'] if sel('#venue_lat') else ''
        event['lon'] = sel('#venue_lng')['value'] if sel('#venue_lng') else ''
        event['geolocation'] = f"{event['lat']},{event['lon']}"

        terms = soup.select('#tnc ul li')
        event['policyAndConditions'] = '\n'.join([f"{i+1}. {t.get_text(strip=True)}" for i, t in enumerate(terms)])
        for t in terms:
            txt_lower = t.get_text().lower()
            if '21+' in txt_lower: event['minimumAge'] = "21"
            if 'alcohol' in txt_lower: event['alcoholServed'] = 'Yes'
            if 'smoking' in txt_lower: event['smokingAllowed'] = 'Yes'
            if 'parking' in txt_lower: event['parkingAvailable'] = 'Yes'
            if 'food' in txt_lower: event['foodAvailable'] = 'Yes'
            if 'wheelchair' in txt_lower: event['wheelchairAccess'] = 'Yes'

        json_file = os.path.join(OUTPUT_DIR, f"event_{event['eventId']}.json")
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump({"eventId": event['eventId'], "externalEventLink": url, "rawHtml": raw_html}, f, indent=2)
        event['jsonFilePath'] = json_file

        plur_json = build_plur_json(event)
        event['Plur_json_format'] = json.dumps(plur_json, ensure_ascii=False, indent=2)
        ALL_PLUR_JSONS.append(plur_json)

        del event['rawHtml']
        SUMMARY["success"] += 1
        return event
    except Exception as e:
        SUMMARY["failed"] += 1
        SUMMARY["failed_links"].append(url)
        print(f"❌ Failed to scrape: {url}\n{e}")
        return None

async def main():
    input_csv = input("📄 Enter the input CSV file name: ").strip()
    column_name = input("🔍 Enter the column name containing event links: ").strip()

    if not os.path.exists(input_csv):
        print(f"❌ File not found: {input_csv}")
        return

    df_links = pd.read_csv(input_csv)
    if column_name not in df_links.columns:
        print(f"❌ Column '{column_name}' not found in CSV")
        return

    urls = df_links[column_name].dropna().unique()
    SUMMARY["total"] = len(urls)
    print(f"🚀 Starting to scrape {len(urls)} events...\n")

    enriched_events = []
    for url in urls:
        event = await scrape_highape_event(url)
        if event:
            enriched_events.append(event)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv = f"highape_events_enriched_{timestamp}.csv"
    pd.DataFrame(enriched_events).to_csv(output_csv, index=False, quoting=1)

    with open(f"highape_events_structured_{timestamp}.json", "w", encoding="utf-8") as f:
        json.dump(ALL_PLUR_JSONS, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Scraping completed. CSV saved as: {output_csv}")
    print(f"📊 Summary: {SUMMARY['success']} succeeded, {SUMMARY['failed']} failed out of {SUMMARY['total']}")
    if SUMMARY['failed']:
        print("❗ Failed links:")
        for link in SUMMARY['failed_links']:
            print(f"- {link}")

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
