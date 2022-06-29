"""Parse a Wikipedia in bulgarian to have a dataset for entity linking tasks.


Download BG Wikipedia
https://dumps.wikimedia.org/bgwiki/20220620/
2022-06-20 17:28:49 done Articles, templates, media/file descriptions, and primary meta-pages.
bgwiki-20220620-pages-articles.xml.bz2 373.8 MB

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import hashlib
import logging
import os
import re
import urllib
from xml.etree import ElementTree as et

import mwparserfromhell as mwp
import pandas as pd
from absl import app
from absl import flags

flags.DEFINE_string(
    "bgwiki_archive", None,
    "The path of the bgwiki  listed on "
    "https://dumps.wikimedia.org/bgwiki/20220620/ with "
    "filename bgwiki-20220620-pages-articles.xml.bz2.")
flags.DEFINE_string(
    "output_dir", None,
    "The directory in which to place output files.")
flags.DEFINE_string(
    "log_level", 'info',
    "Granularity of logs.")
flags.DEFINE_integer(
    "max_pages", 5,
    "How many pages to process.")
flags.DEFINE_integer(
    "limit_mentions_per_page", 20,
    "How many mentions per page to leave, others are ignored.")

FLAGS = flags.FLAGS

MENTIONS_CSV = "mentions.csv"
ENTITIES_CSV = "entities.csv"



def wiki_encode(url):
    """URLEncode a URL (or URL component) to the format used by Wikipedia.
    Args:
      url: The URL (or URL component) to encode.
    Returns:
      The URL with illegal characters %-encoded and spaces turned to underscores.
    """
    return urllib.parse.quote(url.replace(" ", "_"), ";@$!*(),/:")


class MentionRecord(object):
    def __init__(self, link, left_text, text_len_link):
        self.link = link
        self.left_text = left_text
        self.right_text = u""
        left_words = re.findall(r'\w+', left_text)[-5:]
        self.left_text = " ".join(left_words)
        self.text_len_link = text_len_link
        self.grab_right_text_pos = len(left_text) + 1 + text_len_link
        self.left_ok = len(left_words) > 3
        self.right_ok = False

    def set_whole_text(self, text):
        # If this is the right text just added then
        # the link is coming again and needs to be cut from text
        self.right_text = text[self.grab_right_text_pos:]
        right_words = re.findall(r'\w+', self.right_text)[:5]
        self.right_text = " ".join(right_words)
        self.right_ok = len(right_words) > 3

    def __repr__(self):
        return f"{self.left_text} [{self.link}] {self.right_text}"

    def is_complete(self):
#         return self.left_ok and self.right_ok
        return self.right_ok

class EntityRecord(object):
    def __init__(self, title, text, url):
        self.title = title
        text = text.strip()
        if "." in text:
            self.text = text[:text.index('.')]
        else:
            self.text = text[:20]
        self.url = url

    def __repr__(self):
        return f"{self.title}::{self.text}::{self.url}"


class BgWikiParser(object):
    """A class for parsing the contents of a Wikinews archive."""

    def __init__(self):
        self.mentions_df = pd.DataFrame(columns=['left_context','link_title','link_text','right_context','url','mention_in_page'])
        self.entities_df = pd.DataFrame(columns=['title','text','url'])

    def extract_docs(self):
        # , doc_index):
        """Extract entities and mentions from the BG Wikipedia snapshot."""
        logging.info("Extracting at most [%s] docs from [%s]", FLAGS.max_pages, FLAGS.bgwiki_archive)
        ns = {"mw": "http://www.mediawiki.org/xml/export-0.10/"}

        processed_pages = 0
        with open(FLAGS.bgwiki_archive, "rb") as bf:
            # with bz2file.BZ2File(bf) as xf:
            #     parser = et.iterparse(xf)
            parser = et.iterparse(bf)

            # Hold on to the root element so that we can clear empty children that
            # pile up as we incrementally parse through the XML file.
            _, root = next(parser)

            for _, elem in parser:
                if not elem.tag.endswith("/}page"):
                    continue

                # Skip articles that are not published.
                text_elem = elem.find("mw:revision/mw:text", ns)
                if (text_elem is None or text_elem.text is None):
                    elem.clear()
                    root.clear()
                    continue

                # Extract the wikitext from the archive.
                title = elem.find("mw:title", ns).text
                encoded_title = wiki_encode(title)
                url = "https://bg.wikipedia.org/wiki/" + encoded_title
                # encoded_doc = text_elem.text.encode("utf-8")
                encoded_doc = text_elem.text
                if ":" in title:
                    logging.info("Skipping page: [%s]", title)
                    elem.clear()
                    root.clear()
                    continue

                # These clear() calls release references to the XML nodes, preventing
                # unbounded memory consumption while performing streaming XML parsing.
                elem.clear()
                root.clear()
                #print('TITLE: ', title)
                #print('URL: ', url)
                #print("DOC:", encoded_doc)
                (parsed_text, mentions) = self._parse_doc(encoded_doc)
                #print("DOC2:", parsed_text)

                entity = EntityRecord(title,parsed_text, url)
                self.entities_df = self.entities_df.append({
                                        'title': entity.title,
                                        'text': entity.text,
                                        'url': entity.url
                                    }, ignore_index = True)

                mentions = list(filter(lambda x: x.is_complete(), mentions))
                if len(mentions) > FLAGS.limit_mentions_per_page:
                    logging.info("Limiting mentions from %d to %d", len(mentions), FLAGS.limit_mentions_per_page)
                    mentions = mentions[:FLAGS.limit_mentions_per_page]

                for m in mentions:
                    self.mentions_df = self.mentions_df.append({
                        'left_context':m.left_text,
                        'link_title':m.link.title,
                        'link_text':m.link.text,
                        'right_context':m.right_text,
                        'mention_in_page': title
                    }, ignore_index = True)

                processed_pages += 1
                if processed_pages > FLAGS.max_pages:
                    break

        # Persist mentions to CSV
        self.mentions_df.to_csv(MENTIONS_CSV, index_label='idx')
        self.entities_df.to_csv(ENTITIES_CSV,  index_label='idx')

    def _parse_doc(self, wiki_doc):
        mentions = []
        """Parse wiki_doc to produce a text document and a set of mention spans."""
        output = u""

        # 2018-11-29_00_Wikinews: The parser treats DISPLAYTITLE as text rather than
        # a template. For now, get rid of titles. Later consider cleaning them of
        # tags and putting them at the top of each doc.
        wiki_doc = re.sub(r"\{\{DISPLAYTITLE.*?\}\}\s*", r"", wiki_doc)

        # 2018-03-13_00_Mirror: Wikitext represents italics with double-quote ('')
        # and bold with triple-quote ('''). The parser we use gets confused by the
        # edge case where an italicized word is followed by an apostrophe-s
        # (e.g. ''Mirror'''s). We work around this by preprocessing the document
        # with two regular expressions, one to remove bold tags and one to remove
        # italics tags.
        #
        # First, remove all bold tags. Remove them as matching pairs to avoid
        # breaking the "Mirror's" case above.
        wiki_doc = re.sub(r"([^'])'''([^']+)'''([^'])", r"\1\2\3", wiki_doc)
        # Next, remove remove all italics tags. Remove them as singular tags, to
        # handle edge cases where the italics tags are not properly terminated, as
        # in 2018-07-20_00_Hindi.
        wiki_doc = re.sub(r"''", r"", wiki_doc)

        # 2018-11-29_00_Wikinews: The parser treats the table of contents tag as
        # text, so remove it before parsing.
        wiki_doc = re.sub(r"__TOC__\s*", r"", wiki_doc)
        parsed = mwp.parse(wiki_doc)

        # Remove tags or replace them with their contents.
        for node in parsed.filter_tags(recursive=True):
            try:
                if (node.tag == "mapframe" or  # 2018-01-29_01_Afghanistan
                        node.tag == "table" or  # 2018-07-31_01_Total
                        node.tag == "blockquote" or  # 2018-04-04_00_US
                        node.tag == "div" or  # 2018-06-13_01_Tennis
                        node.tag == "gallery"):  # 2018-06-01_00_Photo
                    # This tag does not have a sensible text representation. Remove it.
                    logging.debug("removing tag [%s]", str(node))
                    parsed.remove(node)
                else:
                    # Replace remaining tags with their contents.
                    logging.debug("replacing tag [%s] with [%s]", str(node),
                                  str(node.contents))
                    parsed.replace(node, node.contents)
            except ValueError:
                # This node's parent was already removed. Skip it.
                continue

        # Process templates. Remove some, replacing others with their contents, and
        # leave a few in place for producing output later.
        for node in parsed.filter_templates(recursive=True):
            try:
                node_name = str(node.name).lower()
                if node_name == "translated quote":  # 2018-01-20_01_Ukraine
                    replacement = node.params[-1].value
                    logging.debug("replacing translated quote: [%s] with [\"%s\"]",
                                  str(node), str(replacement))
                    parsed.insert_before(node, "\"")
                    parsed.insert_after(node, "\"")
                    parsed.replace(node, replacement)
                elif node_name == "translation note":  # 2018-01-27_01_India
                    replacement = node.params[0].value
                    logging.debug("replacing translation note [%s] with [%s]", str(node),
                                  str(replacement))
                    parsed.replace(node, replacement)
                elif node_name == "nowrap":  # 2018-06-29_00_Dutch
                    replacement = node.params[0].value
                    logging.debug("replacing nowrap [%s] with [%s]", str(node),
                                  str(replacement))
                    parsed.replace(node, replacement)
                elif node_name == "wikt":  # 2018-03-28_00_K
                    replacement = node.params[-1].value
                    logging.debug("replacing wikt [%s] with [%s]", str(node),
                                  str(replacement))
                    parsed.replace(node, replacement)
                elif node_name == "ft to m":  # 2018-02-07_02_SpaceX
                    ft = float(str(node.params[0].value))
                    m = ft * 0.3048
                    replacement = "{:0.0f}&nbsp;feet ({:0.1f}&nbsp;m)".format(ft, m)
                    logging.debug("replacing ft to m [%s] with [%s]", str(node),
                                  str(replacement))
                    parsed.replace(node, replacement)
                elif node_name == "mi to km":  # 2018-10-12_00_Manned
                    mi = float(str(node.params[0].value))
                    km = mi * 1.60934
                    replacement = "{:0.0f}&nbsp;miles ({:0.0f}&nbsp;km)".format(mi, km)
                    logging.debug("replacing mi to km [%s] with [%s]", str(node),
                                  str(replacement))
                    parsed.replace(node, replacement)
                elif node_name in ["date", "w", "hys", "haveyoursay"]:
                    # These templates are used to produce output below.
                    pass
                else:
                    # Remove all other templates.
                    logging.debug("removing template %s", str(node))
                    parsed.remove(node)
            except ValueError:
                # This node's parent was already removed. Skip it.
                continue

        # Replace HTML elements with their normalized form.
        for node in parsed.filter_html_entities(recursive=True):
            replacement = node.normalize()
            logging.debug("replacing html entity [%s] with [%s]", str(node),
                          str(replacement))
            parsed.replace(node, replacement)

        # Remove all comments.
        for node in parsed.filter_comments(recursive=True):
            parsed.remove(node)

        # Generate text from nodes that remain.
        for node in parsed.ifilter(recursive=False):
            if isinstance(node, mwp.nodes.template.Template):
                node_name = node.name.lower()
                if node_name == "date":
                    logging.info("encoding date: [%s]", str(node))
                    assert len(node.params) == 1
                    # The date should be the first article content.
                    output = output.rstrip()
                    assert not output, output
                    output += datetime.datetime.strptime(
                        str(node.params[0]), "%B %d, %Y").strftime("%A, ")
                    output += str(node.params[0]) + "\n\n"
                elif node_name == "w":
                    # A wikipedia link.
                    logging.info("encoding w: [%s]", str(node))
                    unnamed_params = [n for n in node.params if "=" not in n]
                    title = str(unnamed_params[0].value)
                    text = (
                        str(unnamed_params[-1].value)
                        if len(unnamed_params) >= 2 else title)
                    output += text
                else:
                    # 2018-03-03_00_French: Have an opinion on this story? Share it!
                    # End of article content. Stop parsing.
                    assert node_name == "hys" or node_name == "haveyoursay"
                    break
            elif isinstance(node, mwp.nodes.heading.Heading):
                logging.info("encoding heading: [%s]", str(node))
                title = str(node.title).strip()
                # These headings indicate the end of article content. Stop parsing.
                if title.lower() in [
                    "interviews",  # 2018-11-29_00_Wikinews
                    "related news",  # 2018-03-28_02_Toronto
                    "sources"
                ]:  # 2018-03-24_00_Charles
                    break
                # 2018-06-16_00_FIFA: Other headings denote new paragraphs.
                output = output.rstrip() + "\n\n" + title + "\n\n"
            elif isinstance(node, mwp.nodes.template.Text):
                # Append the text to the output.
                text = str(node.value)
                logging.info("encoding text: [%s]", str(text))
                output += text
                # Normalize whitespace by eliminating trailing whitespace and more than
                # two consecutive newlines.
                output = re.sub(r" *\n", r"\n", output)
                output = re.sub(r"\n\n\n*", r"\n\n", output)
            elif isinstance(node, mwp.nodes.wikilink.Wikilink):
                logging.info("encoding link: [%s]", str(node))
                title = str(node.title)
                if title.lower().startswith("file:"):  # 2018-01-03_00_Scaffolding
                    continue
                link_repr = str(node.text) if node.text else title
                # Mention should be constructed before the link representation is
                # added to the output text
                mentions.append(MentionRecord(node, output, len(link_repr)))
                output += link_repr
            else:
                logging.fatal("Unrecognized %s node: %s", type(node), str(node))

        output = output.rstrip() + "\n\n"

        for m in mentions:
            m.set_whole_text(output)
            logging.debug("Mention:\t[%s]", repr(m))
        return (output, mentions)

    def parse_docs(self, doc_index):
        """Parse files from wiki_dir to populate text_dir."""
        if os.path.exists(self._text_dir):
            logging.info("Skipping parsing, text dir exists: [%s]", self._text_dir)
            return
        else:
            logging.info("Creating text dir: [%s]", self._text_dir)
            os.mkdir(self._text_dir)

        logging.info("Parsing docs from [%s]", self._wiki_dir)

        for _, row in doc_index.iterrows():
            with open(os.path.join(self._wiki_dir, row["docid"]), "rb") as f:
                doc = self._parse_doc(f.read().decode("utf-8")).encode("utf-8")

            # Verify that the parser produced the expected output text.
            assert row["text_md5"] == hashlib.md5(
                doc).hexdigest(), "output text checksum failure for: " + row["title"]

            with open(os.path.join(self._text_dir, row["docid"]), "wb") as f:
                f.write(doc)

    def load_mention_index(self):
        """Load the mention index dataframe from a TSV file."""
        assert os.path.exists(self._mention_index_path)

        logging.info("Reading mention index from: [%s]", self._mention_index_path)
        return pd.read_csv(
            self._mention_index_path, sep="\t", dtype=str, encoding="utf-8")

    def verify_mentions(self, mention_index):
        """Verify that each mention exists in the correct text location."""
        logging.info("Verifying that mentions appear in text docs")

        for _, row in mention_index.iterrows():
            with open(os.path.join(self._text_dir, row["docid"]), "rb") as f:
                doc = f.read().decode("utf-8")

            # Verify that the mention appears at the correct location in the doc.
            pos = int(row["position"])
            length = int(row["length"])
            mention_span = doc[pos:pos + length]
            assert mention_span == row["mention"], (mention_span, row["mention"])

        logging.info("All mentions appear as expected. Done!")


def main(argv):
    if len(argv) > 1:
        raise app.UsageError("Too many command-line arguments.")
    print("log level:" + FLAGS.log_level)
    logging.basicConfig(level=FLAGS.log_level)
    parser = BgWikiParser()
    parser.extract_docs()


if __name__ == "__main__":
    flags.mark_flag_as_required("bgwiki_archive")
    # flags.mark_flag_as_required("output_dir")
    app.run(main)
