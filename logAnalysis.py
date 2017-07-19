#!/usr/bin/env python2.7
#
# This is a log analysis that querys the news db
# queries from the articles, author, and log tables
# @author Hai Vo
import psycopg2


class LogAnalysis(object):
    DBNAME = "news"

    # Gets the top count of articles default to 3
    # sample output [(Candidate is jerk, alleges rival, 338647)]
    def most_popular_articles(self, count=3):
        db = psycopg2.connect(database=self.DBNAME)
        c = db.cursor()
        sql = """
        SELECT articles.title, COUNT(log.path)
        FROM log
        JOIN articles ON log.path = CONCAT('/article/', articles.slug)
        GROUP BY articles.title
        ORDER BY count
        DESC LIMIT {:d};""".format(count)
        c.execute(sql)
        articles = c.fetchall()
        db.close()
        return articles

    # Get the author and the amount of views they have
    # sample output [('Ursula La Multa', Decimal('507594'))]
    def author_view(self):
        db = psycopg2.connect(database=self.DBNAME)
        c = db.cursor()
        sql = """
        SELECT authors.name, SUM(articleCount.count) as views
        FROM authors ,
            (SELECT articles.author, COUNT(log.path)
            FROM log
            JOIN articles ON log.path = CONCAT('/article/', articles.slug)
            GROUP BY articles.author) AS articleCount
        WHERE authors.id = articleCount.author
        GROUP BY authors.name
        ORDER BY views
        DESC ;"""
        c.execute(sql)
        authors = c.fetchall()
        db.close()
        return authors

    # Return the dates that have more than 1 percent errors
    # sample output [('Jul 17, 2016', Decimal('2.32'))]
    def daily_error(self):
        db = psycopg2.connect(database=self.DBNAME)
        c = db.cursor()
        sql = """
        SELECT to_char(badCount.day, 'Mon DD, YYYY'),
            round(CAST(badCount.count::float /
            goodCount.count * 100 AS numeric), 2)
            AS "percent"
        FROM (SELECT date_trunc('day', time) AS "day", COUNT(*)
              FROM log
              WHERE status NOT LIKE '2%'
              GROUP BY day)
              AS badCount,
             (SELECT date_trunc('day', time) AS "day", COUNT(*)
              FROM log
              WHERE status LIKE '2%'
              GROUP BY "day")
              AS goodCount
        WHERE badCount.day = goodCount.day
        AND (badCount.count::float / goodCount.count * 100) > 1
        GROUP BY badCount.day, badCount.count, goodCount.count;"""
        c.execute(sql)
        errors = c.fetchall()
        db.close()
        return errors

    # Transform return queries to easy to read output
    def output_logs(self):
        top_articles = self.most_popular_articles(3)
        authors_views = self.author_view()
        log_errors = self.daily_error()
        print "---------------------------------------------------"
        print "TOP 3 Articles"
        print "---------------------------------------------------"
        for title, view in top_articles:
            print "%s -- %d views" % (title, view)
        print "\n"

        print "---------------------------------------------------"
        print "All Authors' Views"
        print "---------------------------------------------------"
        for author, view in authors_views:
            print "%s -- %d views" % (author, view)
        print "\n"

        print "---------------------------------------------------"
        print "Days with Error > 1%"
        print "---------------------------------------------------"
        for date, percent in log_errors:
            print date + " -- " + str(percent) + "% errors"
        print "\n"

if __name__ == "__main__":
    test = LogAnalysis()
    test.output_logs()
