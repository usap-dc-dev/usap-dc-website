import psycopg2
import psycopg2.extras
import json


config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())

def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def removeEmail(nameWithEmail):
    words = nameWithEmail.strip().split(" ")
    nameWords = list(filter(lambda word: "@" not in word, words))
    return " ".join(nameWords)

getMistakesQuery = "SELECT award, copi FROM award WHERE copi LIKE '%@%'"
fixMistakesTemplate = "UPDATE award SET copi=%s WHERE award=%s"
def fixMistakesInfo(row):
    award = row["award"]
    copi = row["copi"]
    names = copi.split(";")
    namesWithoutEmails = list(map(removeEmail, names))
    if namesWithoutEmails != names:
        print("Fixing award", award + ":", "copi(s):", copi)
    return {"award": award, "copi": "; ".join(namesWithoutEmails).replace("(Former)","").strip()}
    

(conn, cur) = connect_to_db()
cur.execute(getMistakesQuery)
mistakes = cur.fetchall()
for mistakeRow in mistakes:
    fixedInfo = fixMistakesInfo(mistakeRow)
    fixMistakesQuery = cur.mogrify(fixMistakesTemplate, (fixedInfo["copi"], fixedInfo["award"]))
    print(fixMistakesQuery)
    cur.execute(fixMistakesQuery)
conn.commit()