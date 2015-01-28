package net.slisenko.stackexchange.util;

import net.slisenko.stackexchange.model.StackExchangePost;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;

public class StackExchangePostParser extends AbstractIterableParser<StackExchangePost> {

    private HtmlStripper stripper;

    private XPathExpression postBodyXPath;
    private XPathExpression postTitleXPath;
    private XPathExpression postIdXPath;

    private DocumentBuilder documentBuilder;

    public StackExchangePostParser(Iterator<String> iterator) {
        super(iterator);

        try {
            XPathFactory factory = XPathFactory.newInstance();
            XPath xpath = factory.newXPath();
            postBodyXPath = xpath.compile("/row/@Body");
            postTitleXPath = xpath.compile("/row/@Title");
            postIdXPath = xpath.compile("/row/@Id");

            DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
            domFactory.setNamespaceAware(true);
            documentBuilder = domFactory.newDocumentBuilder();

            stripper = new HtmlStripper();
        } catch (XPathException | ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StackExchangePost parse(String value) {
        try {
            StackExchangePost post = new StackExchangePost();

            Document doc = documentBuilder.parse(new InputSource(new StringReader(value.toString())));

            String title = (String) postTitleXPath.evaluate(doc, XPathConstants.STRING);
            post.setTitle(stripper.stripTags(title));

            String text = (String) postBodyXPath.evaluate(doc, XPathConstants.STRING);
            post.setText(stripper.stripTags(text));

            post.setId(Long.parseLong((String) postIdXPath.evaluate(doc, XPathConstants.STRING)));

            return post;
        } catch (XPathExpressionException | IOException | SAXException e) {
            throw new RuntimeException(e);
        }
    }
}