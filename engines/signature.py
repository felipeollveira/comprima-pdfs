import PyPDF2

def has_signature(pdf_path):
    with open(pdf_path, "rb") as f:
        reader = PyPDF2.PdfReader(f)
        for page in reader.pages:
            if "/Annots" in page:
                annots = page["/Annots"]
                for annot in annots:
                    obj = annot.get_object()
                    if obj.get("/Subtype") == "/Widget" and obj.get("/FT") == "/Sig":
                        return True
    return False