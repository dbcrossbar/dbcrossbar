% A Prolog implementation of the planner.
%
% See https://github.com/dbcrossbar/dbcrossbar/issues/186 for example queries
% and motivation.

% Basic data formats we (hypothetically) support.
simpleFormat(csv).
simpleFormat(avro).

% Compression formats we support.
compressedFormat(gz).
compressedFormat(sz).

% Formats used for a single data stream.
streamFormat(F) :-
    simpleFormat(F).
streamFormat(compressed(F, C)) :-
    simpleFormat(F),
    compressedFormat(C).

% Transfer formats.
format(F) :-
    streamFormat(F).
format(streams(F)) :-
    streamFormat(F).

% Simple stream conversion.
convertor(csv, avro, csv2avro).
convertor(avro, csv, avro2csv).

% PostgreSQL.
convertor(postgres, csv, pgCopyOut).
convertor(csv, postgres, pgCopyIn).

% BigQuery.
convertor(bigquery, gs(streams(csv)), bqExtract).
convertor(gs(streams(csv)), bigquery, bqLoad).

% Google Cloud Storage.
convertor(gs(F), F, gsCpOut) :-
    format(F).
convertor(F, gs(F), gsCpIn) :-
    format(F).

% RedShift.
convertor(redshift, s3(streams(csv)), redshiftExtract).
convertor(s3(streams(csv)), redshift, redshiftLoad).

% S3.
convertor(s3(F), F, s3CpOut) :-
    format(F).
convertor(F, s3(F), s3CpIn) :-
    format(F).

% BigML, which has weird I/O constraints.
convertor(s3(csv), bigml(source), bigmlCreateSourceFromS3).
convertor(s3(streams(csv)), bigml(streams(source)), map(bigmlCreateSourceFromS3)).
convertor(gs(csv), bigml(source), bigmlCreateSourceFromGs).
convertor(gs(streams(csv)), bigml(streams(source)), map(bigmlCreateSourceFromGs)).
convertor(bigml, csv, bigmlDownloadSource).
convertor(bigml(source), bigml(dataset), bigmlCreateDataset).
convertor(bigml(streams(source)), bigml(streams(dataset)), map(bigmlCreateDataset)).

% Compressors.
convertor(F, compressed(F, gz), gzip) :-
    simpleFormat(F).
convertor(compressed(F, gz), F, gunzip) :-
    simpleFormat(F).
convertor(F, compressed(F, sz), snappy) :-
    simpleFormat(F).
convertor(compressed(F, sz), F, unsnapy) :-
    simpleFormat(F).

% Concatenating and splitting streams.
convertor(streams(csv), csv, csvcat).
convertor(csv, streams(csv), csvsplit).

% Mapping over streams.
convertor(streams(F1), streams(F2), map(Convertor)) :-
    streamFormat(F1),
    streamFormat(F2),
    convertor(F1, F2, Convertor).

% Plan a copy.
cp(Source, Dest, Convertors) :-
    cpExcludingSeenTypes(Source, Dest, [], Convertors).

% Copy, exlucluding any types in `SeenTypes`.
cpExcludingSeenTypes(Source, Dest, _, [Convertor]) :-
    convertor(Source, Dest, Convertor).

cpExcludingSeenTypes(Source, Dest, SeenTypes, [Convertor|Convertors]) :-
    % Copy to some `Intermediate` format.
    convertor(Source, Intermediate, Convertor),
    % Abondon this branch of the search if `Intemdiate` is in `SeenTypes`.
    \+ member(Intermediate, SeenTypes),
    % Try to copy from `Intermediate` to `Dest`.
    cpExcludingSeenTypes(Intermediate, Dest, [Intermediate|SeenTypes], Convertors).
