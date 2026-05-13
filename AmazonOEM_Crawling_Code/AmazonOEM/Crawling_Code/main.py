from core.pipeline import Pipeline,Pipeline_1
from workers.picture import PictureDownloader
from core.amazon_url_deduplicator import AmazonURLDeduplicator
if __name__ == "__main__":
    Pipeline().run()

    AmazonURLDeduplicator.run()

    Pipeline_1().run()

    PictureDownloader().run()

