let
    // Define empty fallback schema
    EmptyTable = #table(
        {
            "id", "mis_id", "type", "type_code", "reason", "reason_code", "start_date", "start_session",
            "end_date", "end_session", "sessions", "days", "academic_year", "term", "comments", "agencies_involved",
            "discipline_committee_date", "discipline_committee_result", "discipline_committee_reinstatement_date",
            "discipline_committee_representation_made", "appeal_received", "appeal_result", "appeal_result_date",
            "appeal_reinstatement_date", "created_at", "updated_at", "Student ID"
        },
        {}
    ),

    // Function to retrieve a single page of leaver exclusions
    GetLeaverPage = (page as number) as table =>
    let
        Source = Json.Document(Web.Contents("https://api.wonde.com", [
            RelativePath = "v1.0/schools/A121320526/exclusions/?include=student-leaver&page=" & Number.ToText(page),
            Headers = [Authorization = "47b109ebe1ed7b898e3a32fc7ae506f487287bfa"]
        ])),
        RawData = try Source[data] otherwise {},
        ResultTable = if RawData = null or List.Count(RawData) = 0 then EmptyTable else
            let
                Records = Table.FromList(RawData, Splitter.SplitByNothing(), {"Record"}),
                Expanded = Table.ExpandRecordColumn(Records, "Record", {
                    "id", "mis_id", "type", "type_code", "reason", "reason_code", "start_date", "start_session",
                    "end_date", "end_session", "sessions", "days", "academic_year", "term", "comments", "agencies_involved",
                    "discipline_committee_date", "discipline_committee_result", "discipline_committee_reinstatement_date",
                    "discipline_committee_representation_made", "appeal_received", "appeal_result", "appeal_result_date",
                    "appeal_reinstatement_date", "created_at", "updated_at", "student-leaver"
                }),
                
                // ✅ Safely access student-leaver and filter only valid ones
                ValidRows = Table.SelectRows(Expanded, each 
                    let sl = Record.Field(_, "student-leaver") 
                    in sl <> null and Record.HasFields(sl, "data") and sl[data] <> null
                ),

                LeaverExpanded = Table.ExpandRecordColumn(ValidRows, "student-leaver", {"data"}, {"leaver_data"}),

                // ⬇️ Extract student-leaver.data.id
                IDExtracted = Table.ExpandRecordColumn(LeaverExpanded, "leaver_data", {"id"}, {"Student ID"}),

                Cleaned = Table.SelectRows(IDExtracted, each [Student ID] <> null and Text.Trim([Student ID]) <> ""),
                Normalized = Table.TransformColumns(Cleaned, {{"Student ID", each Text.Upper(Text.Trim(_)), type text}})
            in
                Normalized
    in
        ResultTable,

    // Paginate through all results
    LeaverPages = List.Generate(
        () => [Page = 1, Result = GetLeaverPage(1)],
        each Table.RowCount([Result]) > 0,
        each [Page = [Page] + 1, Result = GetLeaverPage([Page] + 1)],
        each [Result]
    ),

    Combined = Table.Combine(LeaverPages)
in
    Combined
