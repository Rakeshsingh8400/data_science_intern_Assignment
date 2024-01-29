# main.py
import feedparser
from celery import Celery
from sqlalchemy import create_engine, Column, String, DateTime, Integer, Boolean, text
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.exc import IntegrityError
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

app = Celery('news_app', broker='pyamqp://guest:guest@localhost//')

Base = declarative_base()


class NewsArticle(Base):
    __tablename__ = 'news_articles'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    content = Column(String, nullable=False)
    published_date = Column(DateTime, nullable=False)
    source_url = Column(String, nullable=False)
    category = Column(String)
    is_duplicate = Column(Boolean, default=False)

    @declared_attr
    def __table_args__(cls):
        return {'extend_existing': True}

    def __repr__(self):
        return f"<NewsArticle(title='{self.title}', category='{self.category}')>"


def create_database():
    engine = create_engine('postgresql://username:password@localhost/dbname')
    Base.metadata.create_all(engine)
    return engine


def process_article(article):
    # Add your custom logic for article processing, classification, etc.
    sid = SentimentIntensityAnalyzer()
    sentiment_score = sid.polarity_scores(article['content'])['compound']
    if sentiment_score >= 0.2:
        return 'Positive/Uplifting'
    elif 'terrorism' in article['title'].lower() or 'protest' in article['title'].lower() \
            or 'political unrest' in article['title'].lower() or 'riot' in article['title'].lower():
        return 'Terrorism/Protest/Political Unrest/Riot'
    elif 'natural disaster' in article['title'].lower():
        return 'Natural Disasters'
    else:
        return 'Others'


def store_article(session, article):
    try:
        new_article = NewsArticle(
            title=article['title'],
            content=article['content'],
            published_date=func.now(),
            source_url=article['link'],
            category=process_article(article)
        )
        session.add(new_article)
        session.commit()
    except IntegrityError:
        session.rollback()
        session.query(NewsArticle).filter_by(source_url=article['link']).update({"is_duplicate": True})
        session.commit()


def get_rss_feed(feed_url):
    return feedparser.parse(feed_url)


@app.task
def process_rss_feed(feed_url):
    db_engine = create_database()
    session = Session(db_engine)

    feed = get_rss_feed(feed_url)
    for entry in feed.entries:
        store_article(session, entry)

    session.close()


if __name__ == '__main__':
    # Define RSS feed URLs
    rss_feeds = [
        "http://rss.cnn.com/rss/cnn_topstories.rss",
        "http://qz.com/feed",
        "http://feeds.foxnews.com/foxnews/politics",
        "http://feeds.reuters.com/reuters/businessNews",
        "http://feeds.feedburner.com/NewshourWorld",
        "https://feeds.bbci.co.uk/news/world/asia/india/rss.xml"
    ]

    # Process each RSS feed asynchronously
    for feed_url in rss_feeds:
        process_rss_feed.apply_async(args=[feed_url])
