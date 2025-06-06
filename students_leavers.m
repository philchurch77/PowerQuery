let
    // Define fallback empty schema
    EmptyTable = #table(
        {"Student ID", "UPI", "Student Name", "Gender", "SEN Status", "FSM", "EAL", "Pupil Premium", "Year", "Ethnicity"},
        {}
    ),

    // Fetch a single page of leaver students
    GetPage = (page as number) as table =>
    let
        Source = try Json.Document(Web.Contents("https://api.wonde.com", [
            RelativePath = "v1.0/schools/A121320526/students-leaver/?include=extended_details&page=" & Number.ToText(page),
            Headers = [Authorization = "47b109ebe1ed7b898e3a32fc7ae506f487287bfa"]
        ])) otherwise null,

        RawData = try Source[data] otherwise null,

        ResultTable = if RawData = null or List.Count(RawData) = 0 then EmptyTable else
            let
                Records = Table.FromList(RawData, Splitter.SplitByNothing(), {"Record"}),
                Expanded = Table.ExpandRecordColumn(Records, "Record", {
                    "id", "upi", "surname", "forename", "gender", "extended_details"
                }),
                Ext = Table.ExpandRecordColumn(Expanded, "extended_details", {"data"}, {"extended_data"}),
                ExtData = Table.ExpandRecordColumn(Ext, "extended_data", {
                    "ethnicity", "sen_status", "in_lea_care", "ever_in_care",
                    "free_school_meals", "free_school_meals_6", "english_as_additional_language", "premium_pupil_indicator"
                }),
                WithName = Table.AddColumn(ExtData, "Student Name", each [surname] & ", " & [forename]),
                Renamed = Table.RenameColumns(WithName, {
                    {"id", "Student ID"}, {"upi", "UPI"}, {"gender", "Gender"}, {"sen_status", "SEN Status"},
                    {"free_school_meals", "FSM"}, {"english_as_additional_language", "EAL"},
                    {"premium_pupil_indicator", "Pupil Premium"},
                    {"ethnicity", "Ethnicity"}
                }),
                Cleaned = Table.SelectRows(Renamed, each [Student ID] <> null and Text.Trim([Student ID]) <> ""),
                Normalized = Table.TransformColumns(Cleaned, {{"Student ID", each Text.Upper(Text.Trim(_)), type text}})
            in
                Normalized
    in
        ResultTable,

    // Generate list of 100 page numbers
    PageNumbers = List.Numbers(1, 100),

    // Fetch and clean
    AllPages = List.Transform(PageNumbers, each GetPage(_)),
    NonEmpty = List.Select(AllPages, each Table.RowCount(_) > 0),
    Combined = Table.Combine(NonEmpty),
    AddSchoolName = Table.AddColumn(Combined, "School", each "Bassingbourn Village College", type text),
    AddStatus = Table.AddColumn(AddSchoolName, "Status", each "Leaver", type text)
in
    AddStatus
