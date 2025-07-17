# producer Kafka:
# Spark reads the guid list (the artifact IDs) from MinIO.
# KafkaProducer generates (produces) at regular intervals “user_annotation” events on topic kafka user_annotations.
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
    raise RuntimeError("No GUID uploaded by MinIO! Check credentials and path.")
print(f"[AnnotationProducer] Loaded {len(guids)} Unique GUIDs from MinIO.")




KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "user_annotations"

user_ids =[f"user{i:04d}" for i in range(1, 401)] + [
    "museumlover", "artfan21", "picasso88", "anon123", "visitorX",
    "curator42", "historybuff", "artexpert", "randomnick", "user_xyz",
    "artefatto90", "louvrefan", "duomofreak", "restauratore75", "collezionista33",
    "historygeek", "antiquarian", "culturalbuff", "docentX", "digitalcurator", "pippo",
    "pluto", "paperino", "mickeymouse", "donaldduck", "pippofranco", "marco", "franco", "andrea",
    "laura", "sara", "elisa", "giovanni", "nathan", "francesca2", "filippo", "alessandro", "maria", "giulia",
    "luca", "matteo", "chiara", "valentina", "federico", "giuseppe", "antonio", "roberto", "stefano", "claudia",
    "martina", "silvia", "gabriele", "alessia", "riccardo", "francesco", "elena", "michele", "sara2", "giovanna", "paolo", "loredana",
    "caterina", "alberto", "giulia2", "federica", "andrea2", "marta", "simone", "valerio", "chiara2", "giacomo"
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
    "Well-preserved work.", "Interesting engraved details.", "Bright and vivid colors.",
 "The frame is in the original style.", "Restoration evident at the top.",
 "The work shows signs of time.", "Typical of Byzantine art.", "Appears to be from a Venetian school.",
 "The gilding is still bright.", "Very fine carving. ", "Very similar to other Baroque objects.",
 "Uncertain attribution.", "Possible dating to the 14th century.", "Classical religious subject.",
 "Possibly part of a lost triptych.", "Style influenced by German Gothic.",
 "Use of natural pigments evident.", "Unusual form compared to the period.",
 "Traces of overlapping painting.", "Modern restoration visible on left side.", "The background is very detailed.",
 "The subject is very expressive.", "The frame is very ornate.", "The colors are still vibrant.",
 "The work is very detailed.", "The subject is very dynamic.", "The frame is very simple."
] + [f"Simulated comment n. {i}" for i in range(130)] + ["Amazing detail on this sculpture.",
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
    "This belongs in the spotlight.", "looking very good very nice", "awww", "it reminds me of my grandma"]

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
        "guid": random.choice(guids),
        "user_id": random.choice(user_ids),
        "tags": random.sample(tags_pool, k=random.randint(2, 4)),
        "comment": random.choice(comments_pool),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "location": random.choice(locations_pool)
    }
    producer.send(KAFKA_TOPIC, annotation)
    print("Published:", annotation)
    time.sleep(0.5)  # Wait x seconds between publications
