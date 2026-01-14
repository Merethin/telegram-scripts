#!venv/bin/python

import argparse, os, sys, time
from common import open_db, send_telegrams, Telegram

parser = argparse.ArgumentParser()
parser.add_argument('-r', "--region", required=True)
parser.add_argument('-t', "--tgid", required=True)
parser.add_argument('-k', "--key", required=True)
parser.add_argument('-e', "--exclude")
parser.add_argument('-a', "--activity")
parser.add_argument('-m', "--max-endos-given", required=True)
args = parser.parse_args()

client_key = os.getenv("CLIENT_KEY")
if client_key is None:
    print("CLIENT_KEY not provided in the environment!", file=sys.stderr)
    sys.exit(1)

conn = open_db()
cursor = conn.cursor()

if args.activity is None:
    cursor.execute("SELECT canon_name FROM nations_dump WHERE is_wa = TRUE AND region = %s", (args.region, ))
else:
    cursor.execute(
        "SELECT canon_name FROM nations_dump WHERE is_wa = TRUE AND region = %s AND lastlogin > %s", 
        (args.region, int(time.time()) - int(args.activity) * 86400)
    )

result = cursor.fetchall()

exclude = []
if args.exclude is not None:
    exclude = args.exclude.split(",")

nations = [row[0] for row in result if row[0] not in exclude]
telegrams = []

for nation in nations:
    cursor.execute("SELECT COUNT(*) FROM nations_dump WHERE region = %s AND %s = ANY (endorsements)", (args.region, nation))
    endos_given = cursor.fetchone()[0]

    if int(endos_given) < int(args.max_endos_given):
        telegrams.append(Telegram("regional", nation, args.tgid, args.key, client_key))

cursor.close()
conn.close()

send_telegrams(telegrams)