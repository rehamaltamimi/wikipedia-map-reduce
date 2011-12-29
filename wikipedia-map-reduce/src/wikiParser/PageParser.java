package wikiParser;

import static javax.xml.stream.XMLStreamConstants.END_DOCUMENT;

import java.io.InputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.xml.sax.SAXException;

/**
 * A class that parses the edit history of an article from an xml parser.
 * This is a streaming parser - it only deals with one revision at a time.
 * Revisions can be retrieved by calling getNextRevision().
 * 
 * Although revision objects are added to the article object, the text
 * of the revisions is set to null unless storeFullTextInArticle is set
 * to true.  Note that the returned revision object is different from the
 * object stored in the article, and always has the complete revision text.
 * 
 * @author shilad
 *
 */
public class PageParser {

    /**
     * @uml.property  name="reader"
     * @uml.associationEnd
     */
    XMLStreamReader reader;
    /**
     * @uml.property  name="article"
     * @uml.associationEnd
     */
    private Page article;
    /**
     * @uml.property  name="storeFullTextInArticle"
     */
    private boolean storeFullTextInArticle = false;
    /**
     * @uml.property  name="storeRevisionMetadata"
     */
    private boolean storeRevisionMetadata = false;
    /**
     * Whether or not the dataset contains text, or is just stubs.
     */
    private boolean hasText = true;

    /**
     * Constructs a new article parser from an input stream.
     * @param stream
     * @throws SAXException
     * @throws XMLStreamException
     */
    public PageParser(InputStream stream) throws XMLStreamException {
        // get the default factory instance
        XMLInputFactory factory = XMLInputFactory.newInstance();

        // configure it to create readers that coalesce adjacent character sections
        // Setting this would be nice, but it results in a stackoverflow exception.  Grr!
        // factory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);

        factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, Boolean.FALSE);
        this.reader = factory.createXMLStreamReader(stream);
    }

    /**
     * Returns the article associated with the XML stream
     * @return
     * @throws XMLStreamException
     * @uml.property  name="article"
     */
    public Page getArticle() throws XMLStreamException {
        if (article != null) {
            return article;
        }
        String title = searchTextElement("title", true);
        //		System.err.println("title is " + title);
        String id = matchTextElement("id", true);
        article = new Page(title, id);
        return article;
    }

    /**
     * Returns the next revision in the stream, or null if there are none left.
     * @return
     * @throws XMLStreamException
     */
    public Revision getNextRevision() throws XMLStreamException {
        if (reader == null) {
            return null;
        }
        if (article == null) {
            getArticle();
        }
        if (!searchElement("revision", false, true)) {
            reader = null;
            return null;
        }
        String id = matchTextElement("id", true);
        String timestamp = matchTextElement("timestamp", true);
        User contributor = readContributor();
        String minor = matchTextElement("minor", false);
        String comment = matchTextElement("comment", false);
        String vandalism = matchTextElement("isVandalism", false); //FIXME: not right
        String text = null;
        if (hasText) {
            text = matchTextElement("text", true);
            text = stripComments(text, timestamp);
        } else {
            matchElement("text", true);
        }
        boolean isMinor = (minor != null) && minor.equals("1");
        boolean isVandalism = (vandalism != null) && vandalism.equals("1");
        // System.err.println("rev is " + text);

        Revision rev = new Revision(id, timestamp, contributor, text, comment, isMinor, isVandalism);
        if (storeFullTextInArticle) {
            article.addToRevisions(rev);
        } else if (storeRevisionMetadata) {
            article.addToRevisions(new Revision(id, timestamp, contributor, null, comment, isMinor, isVandalism));
        }
        return rev;
    }

    /**
     * FIXME: this doesn't return false early enough
     * @return
     * @throws XMLStreamException
     */
    public boolean hasNextRevision() throws XMLStreamException {
        if (reader == null) {
            return false;
        }
        if (article == null) {
            getArticle();
        }
        if (!searchElement("revision", false, false)) {
            reader = null;
            return false;
        }
        return true;
    }

    /**
     * Returns the contributor for the revision.
     * FIXME: this should return a structured object with
     * either (an id and name) OR an IP address.
     * @return
     * @throws XMLStreamException
     */
    private User readContributor() throws XMLStreamException {
        matchElement("contributor", true);
        String userName = matchTextElement("username", false);
        String ip = matchTextElement("ip", false);
        if (userName != null && ip != null) {
            throw new IllegalStateException("found both ip and username elements");
        } else if (userName != null) {
            String id = matchTextElement("id", true);
            return new User(userName, id);
        } else if (ip != null) {
            return new User(ip);
        } else {		// both must be null
            throw new IllegalStateException("found neither username nor ip after contributor");
        }
    }

    private boolean advance() throws XMLStreamException {
        if (reader.hasNext()) {
            reader.next();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Gets the next opening element.
     * If it has the specified name, return true, and advance the cursor.
     * If eof is reached, or a different element is encountered return false, or
     * if failIfNotFound is set, through an exception.
     *
     * @param name
     * @param failIfNotFound
     * @return
     * @throws XMLStreamException
     */
    private boolean matchElement(String name, boolean failIfNotFound) throws XMLStreamException {
        return matchElement(name, failIfNotFound, true);

    }
    private boolean matchElement(String name, boolean failIfNotFound, boolean advance) throws XMLStreamException {
        while (!reader.isStartElement() && reader.hasNext()) {
            reader.next();
        }
        if (!reader.hasNext() || reader.getEventType() == END_DOCUMENT) {
            if (failIfNotFound) {
                throw new IllegalStateException("received eof while looking for " + name);
            } else {
                return false;
            }
        }
        if (reader.getName().getLocalPart().equals(name)) {
            if (advance) advance();
            return true;
        } else if (failIfNotFound) {
            throw new IllegalStateException("found " + reader.getName() + " while looking for " + name);
        } else {
            return false;
        }
    }

    /**
     * Search for the next element with the specified name.
     * @param name
     * @param failIfNotFound
     * @return
     * @throws XMLStreamException
     */
    private boolean searchElement(String name, boolean failIfNotFound) throws XMLStreamException {
        return searchElement(name, failIfNotFound, true);
    }
    private boolean searchElement(String name, boolean failIfNotFound, boolean advance) throws XMLStreamException {
        while (reader.hasNext() && !matchElement(name, false, advance)) {
            // match against the next element
            advance();
        }
        if (!reader.hasNext() && failIfNotFound) {
            throw new IllegalStateException("received eof while looking for " + name);
        } else if (!reader.hasNext() && !failIfNotFound) {
            return false;
        }
        return true;
    }

    /**
     * Reads the xml stream until the occurence of a particular tag, then
     * returns the textual contents of the tag.
     * @param name
     * @param failIfNotFound if true, through an Exception if parsing fails (instead of returning null).
     * @return
     * @throws XMLStreamException
     */
    private String matchTextElement(String name, boolean failIfNotFound) throws XMLStreamException {
        if (!matchElement(name, failIfNotFound)) {
            return null;
        }
        if (reader.isCharacters()) {
            //			String s = reader.getText();
            //                        System.out.println("text is " + s);
            //			advance();
            //			return s;
            return getCoalescedText();
        } else if (reader.isEndElement()) {
            return "";	// no text element.
        } else if (failIfNotFound) {
            throw new IllegalStateException("no characters found following " + name);
        } else {
            return "";
        }
    }

    /**
     * Reads the xml stream until the occurence of a particular tag, then
     * returns the textual contents of the tag.
     * @param name
     * @param failIfNotFound if true, through an Exception if parsing fails (instead of returning null).
     * @return
     * @throws XMLStreamException
     */
    private String searchTextElement(String name, boolean failIfNotFound) throws XMLStreamException {
        if (!searchElement(name, failIfNotFound)) {
            return null;
        }
        if (reader.isCharacters()) {
            //			String s = reader.getText();
            //			advance();
            //			return s;
            return getCoalescedText();
        } else if (reader.isEndElement()) {
            return "";	// no text element.
        } else if (failIfNotFound) {
            throw new IllegalStateException("no characters found following " + name);
        } else {
            return "";
        }
    }

    public boolean getStoreFullTextInArticle() {
        return storeFullTextInArticle;
    }

    /**
     * @param storeFullTextInArticle
     * @uml.property  name="storeFullTextInArticle"
     */
    public void setStoreFullTextInArticle(boolean storeFullTextInArticle) {
        this.storeFullTextInArticle = storeFullTextInArticle;
    }

    public boolean getStoreRevisionMetadata() {
        return storeRevisionMetadata;
    }

    /**
     * @param storeRevisionMetadata
     * @uml.property  name="storeRevisionMetadata"
     */
    public void setStoreRevisionMetadata(boolean storeRevisionMetadata) {
        this.storeRevisionMetadata = storeRevisionMetadata;
    }
    private static final int MAX_LENGTH = 500000;

    private String getCoalescedText() throws XMLStreamException {
        boolean tooLong = false;
        StringBuilder sb = new StringBuilder();
        while (reader.isCharacters()) {
            String t = reader.getText();
            if (sb.length() < MAX_LENGTH) {
                sb.append(t);
            } else if (!tooLong) {
                tooLong = true;
                System.err.println("PageParser.getCoalescedText(): Text to retrieve was too long.");
            }
            advance();
        }
        return sb.toString();
    }
    private static final String BEGIN_COMMENT = "<!--";
    private static final String END_COMMENT = "-->";

    private String stripComments(String text, String timestamp) {
        StringBuilder stripped = new StringBuilder();
        int i = 0;
        while (true) {
            int j = text.indexOf(BEGIN_COMMENT, i);
            if (j < 0) {
                stripped.append(text.substring(i));
                break;
            }
            stripped.append(text.substring(i, j));
            int k = text.indexOf(END_COMMENT, j);
            // FIXME: handling comments and end_comments could be better
            if (k < 0) {
                System.err.println("no end comment found in " + article.getName() + " (" + timestamp + ")");
                break;
            }
            i = k + END_COMMENT.length();
        }
        return stripped.toString();
    }

    public void setHasText(boolean hasText) {
        this.hasText = hasText;
        if (!hasText) {
            this.storeFullTextInArticle = false;
        }
    }
}
