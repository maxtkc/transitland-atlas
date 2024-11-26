import os
import subprocess

# List of feed URLs
feed_urls = [
"https://passio3.com/Clemson2/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/jpshealth/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/charlesriver/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/sandyor/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/fortsask/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/franklint/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/ecat/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/elonedu/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/kfoothills/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/JCMTD/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/charlestonairport/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/GASO/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/4PointsFlushing/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/macog/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/epta/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/ewrpanynj/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/jaspercan/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/montachusett/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/ethra/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/lax/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/freshdirect/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/harbortown/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/cooscounty/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/mizzst/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/edmond/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/montana/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/ccmc/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/keywest/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/fpark/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/CityofTracy/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/lewiston/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/simonedev/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/hillplace/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/demo2/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/georgiast/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/houstonairport/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/NCI/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/Concourse/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/uniteda/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/lawrencetransit/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/disneyw/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/jbsa/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/longislanduni/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/floridaint/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/buildingland/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/hollinsshuttle/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/Emoryuni/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/puertoricotec/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/jfkpanynj/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/MetroParking/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/davisa/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/otterbus/passioTransit/gtfs/google_transit.zi",
"https://passio3.com/security@colby.edu/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/newportbeach/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/mktrail/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/marymount/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/epsteins/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/hendrycounty/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/boiseairport/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/harrisco/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/lehigh/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/georgewashu/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/gcsu/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/clackamas/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/mit/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/reefdoc/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/cgables/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/orlandoaviation/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/cuats/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/countyconn/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/tylertx/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/fitnewyork/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/bangor/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/nyulangone/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/endicott/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/detroitemp/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/725ponce/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/coke/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/gatewayjfk/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/frta/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/clovis/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/canbyTransit/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/bowie/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/easternken/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/newyork/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/atlcarpark/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/choa/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/calabasas/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/watertown/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/longbeachcal/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/druryhoteldisney/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/charmcity/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/beaconprop/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/casper/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/gru/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/alliance/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/cet/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/ClemsonU/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/beloit/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/century/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/sundiego/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/chemung/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/BeaconCollege11/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/bayonne/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/brockton/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/rwwl/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/AnneC/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/ascott/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/cardinal5025/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/peachtree/passioTransit/gtfs/google_transit.zip",
"https://passio3.com/highlandhos/passioTransit/gtfs/google_transit.zip"
]

# List to store validation results
validation_results = []

def validate_feed(url):
    try:
        result = subprocess.run(["transitland", "validate", url], capture_output=True, text=True, check=True)
        print(f"Validation output for {url}: {result.stdout}")
        validation_results.append((url, "Validated Successfully"))
    except subprocess.CalledProcessError as e:
        print(f"Validation failed for {url}: {e.stderr}")
        validation_results.append((url, "Validation Failed"))

# Iterate over each URL and validate
for url in feed_urls:
    validate_feed(url)

# Display the results
print("\nValidation Results:")
for url, status in validation_results:
    print(f"{url}: {status}")

print("All feeds processed.")
