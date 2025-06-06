let
    // Define the source range
    Source = {1..20},
    
    // Convert the source list to a table and rename the column to "Page"
    ConvertedToTable = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    RenamedColumns = Table.RenameColumns(ConvertedToTable, {{"Column1", "Page"}}),
    
    // Invoke the custom function "Netherhall Attendance OT" on each page
    InvokedCustomFunction = Table.AddColumn(RenamedColumns, "Netherhall Attendance OT", each 
        let
            page = [Page],
            Source = Json.Document(Web.Contents("https://api.wonde.com", [RelativePath="v1.0/schools/A122478746/attendance/session/?attendance_date_after=2024-09-01 10:10:43&attendance_date_before=2024-10-01&page=" & Number.ToText(page), Headers=[#"Authorization"="b32d9c18bc18dc76a6dee47b0d389a52ba0bf8b8"]])),
            Data1 = Source{1}[Data],
            RemoveBottom = Table.RemoveLastN(Data1, 3),
            #"Converted to Table" = Record.ToTable(Source),
            Value = #"Converted to Table"{0}[Value],
            #"Converted to Table1" = Table.FromList(Value, Splitter.SplitByNothing(), null, null, ExtraValues.Error)
        in   
            #"Converted to Table1"),
    
    // Remove rows with errors in the "Netherhall Attendance OT" column
    RemovedErrors = Table.RemoveRowsWithErrors(InvokedCustomFunction, {"Netherhall Attendance OT"}),
    
    // Expand the "Netherhall Attendance OT" column and its nested columns
    ExpandedNetherhallAttendanceOT = Table.ExpandTableColumn(RemovedErrors, "Netherhall Attendance OT", {"Column1"}, {"Netherhall Attendance OT.Column1"}),
    ExpandedNetherhallAttendanceOTColumn1 = Table.ExpandRecordColumn(ExpandedNetherhallAttendanceOT, "Netherhall Attendance OT.Column1", {"date", "session", "attendance_code", "student"}, 
        {"Date", "Session", "Code", "Student"}),
    ExpandedDateColumn = Table.ExpandRecordColumn(ExpandedNetherhallAttendanceOTColumn1, "Date", {"date"}, {"Date"}),
    
    // Change data types
    ChangedType = Table.TransformColumnTypes(ExpandedDateColumn, {{"Date", type datetime}}),
    #"Appended Query" = Table.Combine({ChangedType, #"Netherhall Attendance OT Q (2)", #"Netherhall Attendance OT Q (3)", #"Netherhall Attendance OT Q (4)", #"Netherhall Attendance OT Q (5)", #"Netherhall Attendance OT Q (6)", #"Netherhall Attendance OT Q (7)", #"Netherhall Attendance OT Q (8)", #"Netherhall Attendance OT Q (9)", #"Netherhall Attendance OT Q (10)", #"Netherhall Attendance OT Q (11)"})
in
    #"Appended Query"
