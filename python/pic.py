from elasticsearch_dsl import analyzer, DocType, Object, Nested, String, Integer, Long

accent_analyzer = analyzer('accent_analyzer',
    tokenizer='standard',
    filter = ['lowercase', 'asciifolding']
)

class Constituent(DocType):
    ConstituentID = String()
    DisplayName = String(analyzer=accent_analyzer)
    DisplayDate = String()
    AlphaSort = String()
    Nationality = String()
    BeginDate = String()
    EndDate = String()
    ConstituentTypeID = String()
    addressTotal = Integer()
    nameSort = String()
    TextEntry = String()

    address = Nested(
        include_in_parent=True,
        properties={
            'ConAddressID' : String(),
            'ConstituentID' : String(),
            'AddressTypeID' : String(),
            'DisplayName2' : String(),
            'StreetLine1' : String(),
            'StreetLine2' : String(),
            'StreetLine3' : String(),
            'City' : String(),
            'State' : String(),
            'CountryID' : String(),
            'BeginDate' : String(),
            'EndDate' : String(),
            'Remarks' : String()
        }
    )

    biography = Object(
        properties={
            'TermID' : String(),
            'URL' : String()
        }
    )

    collection = Object(
        properties={
            'TermID' : String(),
            'URL' : String()
        }
    )

    format = Object(
        properties={
            'TermID' : String()
        }
    )

    gender = Object(
        properties={
            'TermID' : String()
        }
    )

    process = Object(
        properties={
            'TermID' : String()
        }
    )

    role = Object(
        properties={
            'TermID' : String()
        }
    )
