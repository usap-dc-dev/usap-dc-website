import lxml.etree as ET
import sys
import os


def makeISOXMLFile(dcxml_filename, xsl_filename, isoxml_filename):
    try:
        dom = ET.parse(dcxml_filename)
        xslt = ET.parse(xsl_filename)
        transform = ET.XSLT(xslt)
        newdom = transform(dom)
        out_text = ET.tostring(newdom, pretty_print=True)

        with open(isoxml_filename, "w") as myfile:
            myfile.write(out_text)
        os.chmod(isoxml_filename, 0o664)

        return "Successfully created ISO XML file %s" % isoxml_filename
    except Exception as e:
        return "Error making ISO XML.  %s" % str(e)


if __name__ == '__main__':
    dcxml_filename = sys.argv[1]
    xsl_filename = sys.argv[2]
    isoxml_filename = sys.argv[3]
    print(makeISOXMLFile(dcxml_filename, xsl_filename, isoxml_filename))
