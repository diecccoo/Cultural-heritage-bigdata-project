# producer Kafka:
# Spark legge la lista di guid (gli ID degli artefatti) da MinIO.
# KafkaProducer genera (produce) a intervalli regolari eventi di “user_annotation” sul topic kafka user_annotations.

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("UserAnnotationsProducer")
    .config("spark.jars.packages",
              "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4")
      # (facoltativo) abilitazione estensioni Delta
      # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      # MinIO / S3A
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()
)

guids = (spark.read.format("delta")
    .load("s3a://heritage/cleansed/europeana")
    .select("guid").distinct()
    .rdd.map(lambda r: r[0]).collect())
spark.stop()

if not guids:
    raise RuntimeError("Nessun GUID caricato da MinIO! Controlla credenziali e path.")
print(f"[AnnotationProducer] Caricati {len(guids)} GUID unici da MinIO.")




KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "user_annotations"

user_ids =[f"user{i:04d}" for i in range(1, 401)] + [
    "museumlover", "artfan21", "picasso88", "anon123", "visitorX",
    "curator42", "historybuff", "artexpert", "randomnick", "user_xyz",
    "artefatto90", "louvrefan", "duomofreak", "restauratore75", "collezionista33",
    "historygeek", "antiquarian", "culturalbuff", "docentX", "digitalcurator", "pippo",
    "pluto", "paperino", "mickeymouse", "donaldduck", "pippofranco",
]

tags_pool = [
    "religious", "sculpture", "wood", "metal", "ceramic", "glass", "bronze", "marble",
    "baroque", "gothic", "renaissance", "medieval", "modern", "contemporary", "neoclassical",
    "abstract", "portrait", "landscape", "still life", "historical", "mythological", "biblical",
    "inscription", "calligraphy", "figurative", "realistic", "symbolic", "engraved", "carved",
    "painted", "restored", "fragment", "intact", "gilded", "damaged", "framed", "mounted", "panel",
    "altarpiece", "triptych", "diptych", "icon", "relief", "tapestry", "mosaic", "fresco", "illumination",
    "bookbinding", "parchment", "manuscript", "textile", "costume", "armor", "tool", "ritual",
    "architectural", "decorative", "archaeological", "ornament", "frame", "vessel", "utensil",
    "clay", "ivory", "stone", "ink", "pastel", "oil", "tempera", "watercolor", "charcoal", "chalk",
    "silver", "gold", "enamel", "plaster", "terracotta", "papier-mâché", "lacquer", "resin", "bone"
]

comments_pool = [
    "Opera ben conservata.", "Interessanti dettagli incisi.", "Colori vivaci e luminosi.",
    "La cornice è in stile originale.", "Restauro evidente nella parte superiore.",
    "L’opera presenta segni del tempo.", "Tipico dell’arte bizantina.", "Sembra provenire da una scuola veneziana.",
    "La doratura è ancora brillante.", "Intaglio molto fine.", "Molto simile ad altri oggetti barocchi.",
    "Attribuzione incerta.", "Possibile datazione al XIV secolo.", "Soggetto religioso classico.",
    "Forse parte di un trittico perduto.", "Stile influenzato dal gotico tedesco.",
    "Utilizzo di pigmenti naturali evidente.", "Forma inconsueta rispetto all'epoca.",
    "Tracce di pittura sovrapposta.", "Restauro moderno visibile sul lato sinistro."
] + [f"Commento simulato n. {i}" for i in range(130)] + ["Amazing detail on this sculpture.",
    "Looks like late Baroque style.", "Still vibrant after all these years.",
    "Possibly restored in recent decades.","One of the most beautiful pieces I've seen.",
    "Looks like it's been part of a larger altar.","Very expressive facial features.",
    "The frame seems original.", "So much texture in the background.",
    "Interesting use of gold leaf.", "Could this be from the Venetian school?",
    "Nice patina on the bronze.", "Reminds me of something from Florence.",
    "Highly decorative.", "Elegant lines and symmetry.", "Unusual choice of colors for the time.",
    "You can really feel the emotion.", "Might be influenced by northern artists.",
    "The lighting enhances the depth.", "Could be part of a diptych.",
    "Extremely fine engravings.","Clearly inspired by classical antiquity.",
    "Soft brushwork, almost impressionistic.", "This deserves more attention.",
    "Why is there no author listed?", "Remarkably well-preserved.",
    "Incredible texture and layering.", "Such delicate hands!",
    "I wonder what the inscription says.", "Color tones remind me of Fra Angelico.",
    "Very lifelike representation.", "I love the way it's mounted.",
    "Looks like a funerary object.",  "Might be from a private collection.",
    "Not sure about the attribution here.",  "Some parts appear reconstructed.",
    "Gothic influence is clear.", "This belongs in a larger museum.",
    "Color fading is evident but beautiful.",  "Interesting depiction of the divine.",
    "I'd love to know its provenance.",   "Clearly a devotional object.",
    "The folds in the robe are incredible.",   "Mysterious aura around this piece.",
    "Could be related to a known master.",   "Surprisingly detailed for its size.",
    "Textile work is impressive.",   "The frame is as interesting as the painting.",
    "Very minimalistic for its era.",    "Part of a religious ritual?",
    "I've seen similar in Spain.",    "Rich colors, especially the blues.",
    "Great example of gilding techniques.",    "Would love to see this in person.",
    "It feels unfinished somehow.",    "This tells a story without words.",
    "Broken yet beautiful.",    "Hard to photograph due to reflections.",
    "Slight damage on the upper left.",    "That crack seems to be intentional.",
    "One of the best pieces in this collection.",    "Could be linked to a monastery.",
    "Decorative and spiritual at the same time.",    "Is that lapis lazuli pigment?",
    "Could be a funerary mask.",    "The border pattern is exquisite.",
    "Might be part of a liturgical garment.",    "This needs urgent conservation.",
    "Magnificent facial expression.",    "Rare to see this theme depicted.",
    "Does anyone know the context?",    "The background looks newer.",
    "Color layering is fascinating.",    "Love the dynamic composition.",
    "Seems to have multiple layers of meaning.",    "The signature is hard to read.",
    "Unusual subject for this time period.",    "I'd love to know the full story.",
    "Haunting eyes.",    "Geometric patterns suggest Islamic influence.",
    "Could it be a forgery?",    "Definitely eastern European.",
    "This must have been expensive to produce.",    "Reminds me of stained glass.",
    "A true masterpiece.",    "Who commissioned this, I wonder?",
    "Possibly part of a royal collection.",    "Seems like a relic from a church.",
    "Unclear what the material is.",    "Is that a restoration mark?",
    "The shadow play is brilliant.",    "Beautiful interplay of form and function.",
    "Never seen anything quite like it.",    "So much symbolism packed in one piece.",
    "Looks better in natural light.",    "That facial expression is striking.",
    "Highly polished finish.",    "Interesting pose of the subject.",
    "Seems inspired by mythology.",    "Could be a copy of a lost original.",
    "Color saturation is still strong.",    "Wonderful ornamental details.",
    "Minimal wear considering the age.",    "Likely used in a religious festival.",
    "The eyes seem to follow you.",    "Amazing preservation of pigment.",
    "Detailing on the garment is insane.",    "Wish there was more info about this.",
    "This belongs in the spotlight."]

locations_pool = [
    "Rome", "Florence", "Venice", "Milan", "Naples", "Turin", "Palermo", "Bologna", "Genoa",
    "Trento", "Bolzano", "Paris", "Lyon", "Marseille", "Berlin", "Munich", "Dresden", "Vienna",
    "Salzburg", "Brussels", "Amsterdam", "The Hague", "Madrid", "Barcelona", "Valencia",
    "Lisbon", "Porto", "Athens", "Istanbul", "Cairo", "Alexandria", "London", "Edinburgh",
    "Dublin", "New York", "Chicago", "Los Angeles", "Washington", "Toronto", "Montreal",
    "Mexico City", "Buenos Aires", "Rio de Janeiro", "Tokyo", "Kyoto", "Beijing", "Shanghai",
    "Seoul", "Jakarta", "Delhi", "Mumbai", "Jerusalem", "Damascus", "Baghdad", "Tehran",
    "Prague", "Budapest", "Warsaw", "Krakow", "Copenhagen", "Oslo", "Stockholm", "Helsinki",
    "Zurich", "Geneva", "Brno", "Ljubljana", "Zagreb", "Dubrovnik", "Valletta", "Reykjavik"
]


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

while True:
    annotation = {
        "object_id": random.choice(guids),
        "user_id": random.choice(user_ids),
        "tags": random.sample(tags_pool, k=random.randint(2, 4)),
        "comment": random.choice(comments_pool),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "location": random.choice(locations_pool)
    }
    producer.send(KAFKA_TOPIC, annotation)
    print("Pubblicata:", annotation)
    time.sleep(3)  # Attendi x secondi tra le pubblicazioni
