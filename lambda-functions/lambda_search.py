import json
import boto3
import re
import requests
from requests_aws4auth import AWS4Auth

region = 'us-east-1'
service = 'es'

host = "https://search-photos-4nxovnoosm2qtihvnjxnax7b4i.us-east-1.es.amazonaws.com"

lex = boto3.client("lexv2-runtime")

# Helper functions
def extract_keywords_fallback(query):
    stop_words = {
        "show", "me", "photos", "picture", "pictures", "of",
        "and", "with", "find", "give", "images", "a", "the", "to"
    }

    words = re.findall(r"\w+", query.lower())
    return [w for w in words if w not in stop_words]

def get_keywords_from_lex(query):
    response = lex.recognize_text(
        botId="QHJNBOHKJP",
        botAliasId="TSTALIASID",
        localeId="en_US",
        sessionId="user-session-1",
        text=query
    )

    keywords = []

    try:
        intent = response.get("interpretations", [{}])[0].get("intent", {})
        slots = intent.get("slots", {})

        for slot in slots.values():
            if slot and slot.get("value"):
                keywords.append(slot["value"]["interpretedValue"].lower())
    except Exception:
        pass

    # fallback if Lex returns nothing
    if not keywords:
        keywords = extract_keywords_fallback(query)

    return keywords

def lambda_handler(event, context):
    #print("EVENT:", json.dumps(event))

    # 1. Get query from API Gateway
    query = event.get("queryStringParameters", {}).get("q", "")
    print("Query:", query)

    if not query:
        return {
            "statusCode": 200,
            "body": json.dumps([])
        }

    # 2. Get keywords via Lex
    keywords = get_keywords_from_lex(query)
    print("Keywords:", keywords)

    if not keywords:
        return {
            "statusCode": 200,
            "body": json.dumps([])
        }

    # 3. AWS auth for OpenSearch
    credentials = boto3.Session().get_credentials()

    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        service,
        session_token=credentials.token
    )

    # 4. Build OpenSearch query (fuzzy match)
    search_query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {
                            "labels": {
                                "query": k,
                                "fuzziness": "AUTO"
                            }
                        }
                    }
                    for k in keywords
                ],
                "minimum_should_match": 1
            }
        }
    }

    # 5. Search OpenSearch
    url = f"{host}/photos/_search"

    response = requests.get(
        url,
        auth=awsauth,
        json=search_query,
        headers={"Content-Type": "application/json"}
    )

    print("OpenSearch response:", response.text)

    data = response.json()

    # 6. Build image URLs
    results = []

    for hit in data.get("hits", {}).get("hits", []):
        source = hit["_source"]
        results.append(
            f"https://{source['bucket']}.s3.amazonaws.com/{source['objectKey']}"
        )

    # 7. Return API Gateway response
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(results)
    }
