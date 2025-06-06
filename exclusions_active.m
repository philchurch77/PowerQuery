let
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

    GetActivePage = (page as number) as table =>
    let
        Source = Json.Document(Web.Contents("https://api.wonde.com", [
            RelativePath = "v1.0/schools/A121320526/exclusions/?include=student&page=" & Number.ToText(page),
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
                    "appeal_reinstatement_date", "created_at", "updated_at", "student"
                }),
                ValidRows = Table.SelectRows(Expanded, each [student] <> null and Record.HasFields([student], "data")),
                StudentExpanded = Table.ExpandRecordColumn(ValidRows, "student", {"data"}, {"student_data"}),

                // ⬇️ Extract student.id instead of upi
                IDExtracted = Table.ExpandRecordColumn(StudentExpanded, "student_data", {"id"}, {"Student ID"}),

                Cleaned = Table.SelectRows(IDExtracted, each [Student ID] <> null and Text.Trim([Student ID]) <> ""),
                Normalized = Table.TransformColumns(Cleaned, {{"Student ID", each Text.Upper(Text.Trim(_)), type text}})
            in
                Normalized
    in
        ResultTable,

    AllPages = List.Generate(
        () => [Page = 1, Result = GetActivePage(1)],
        each Table.RowCount([Result]) > 0,
        each [Page = [Page] + 1, Result = GetActivePage([Page] + 1)],
        each [Result]
    ),

    Combined = Table.Combine(AllPages)
in
    Combined
