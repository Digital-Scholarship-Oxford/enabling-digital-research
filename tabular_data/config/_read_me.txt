Tabular Data Processing – Configuration Instructions

Below is an explanation of the information contained within each of the default
	configuration files, as well as details of how to customise the files
	for your own purposes.

1. Notes and General Principles

	1.1. Data Quality

	The Bodleian's Western medieval manuscript catalogues, for which this
		processor is primarily designed, remains a work in progress
		both in the encoding into TEI-XML of data which have already
		been recorded, and in the recording of new data. The absence
		of information regarding e.g. decoration or musical notation
		should therefore not be taken as an indication that no such
		features exist for a given manuscript, only that these data
		have not been recorded in the digital catalogues. Similarly,
		many of the datapoints presented through the outputs of this
		processor are the work of previous generations of librarians
		and scholars who had cataloguing priorities and practices that
		are different to those of modern cataloguers. In particular,
		in previous cataloguing cycles, measurements have often been
		rounded to the nearest 5mm or quarter/eighth of an inch, and
		features such as decoration have been described using
		assessments of aesthetic and material quality (e.g. "Good
		border"), rather than through dispassionate descriptions of
		their form and contents. Users should therefore be aware of
		these limitations when engaging with the outputs of this
		processor, whether using the default outputs or custom ones.

	Both the authority and collection files contain a file/tab on data
		quality, containing details of how many instances of a given
		datapoint appear in the outputs, and what proportion this
		constitutes of the total output. When this absolute or
		percentage figure is low, users should be wary of assuming
		that the feature in question is rare; it is also possible
		that it is just infrequently encoded. For example, the
		relative scarcity of detail concerning script type in the
		TEI catalogue files (and therefore the tabular output) means
		that if there are only a few examples of one type in the
		output, this by no means indicates that these are the only
		such instances in the collections.

	1.2. Order of Processing

	All configuration files will be processed in alphabetical order by filename,
		and this is also the order in which the output tabs will appear
		in the output .xlsx files. If you wish to modify this order,
		include numbers at the beginning of your config filenames.

	Authority files will be processed before collection files, to ensure
		that the data required for any authority lookups have already
		been extracted.

2. Authority Files

	2.1. Source Files

	Any .xml file found in the root directory of the repository will be
		treated as authority files, containing information on entities
		named in the collection files, linked using UIDs. By default
		these are:

		persons.xml	TEI-XML containing details of persons named in
				the collection (e.g. former owners, scribes,
				etc.).

		places.xml	TEI-XML containing details of places and
				organisations named in the collection (e.g.
				place/organisation of origin).

		works.xml	TEI-XML containing details of works associated
				with the collection (usually those contained
				within MSS in the collection).


	2.2. Default Configuration Files

	Any .csv file found in the tabular_data/config/authority directory will
		be treated as configuration files for authority output files.
		By default, these are:

		orgs.csv	Each row of the output file corresponds to an
				organisation named within the authority file
				places.xml.

		persons.csv	Each row of the output file corresponds to a
				person named within the authority file
				persons.xml.

		places.csv	Each row of the output file corresponds to a
				place named within the authority file
				places.xml.

		works.csv	Each row of the output file corresponds to a
				work named within the authority file works.xml.


	2.3. Configuration File Columns

	Individual rows in the .csv configuration file will correspond to
		individual columns in the output file. In order for the
		data extraction to be successful, each .csv configuration
		file must have the following columns:

		section		a top-level heading to group different output
				columns together. Consecutive output columns
				with the same section heading will be grouped
				into a single section within the .xlsx output.
				This field is optional, but advised.

		heading		a heading for the output column, within the
				previously designated section.	

		auth_file	the authority input file (without suffix) from
				which the data should be extracted.

		xpath		the XPath query that should be used to extract
				the data for the present output column. Every
				XPath query in a given authority file must
				return exactly the same number of values as
				any other, else the code will fail. `string()`
				and `string-join()` are good ways of forcing
				multiple (or zero-length) values to be returned
				as a single value. XPath up to and including
				version 3.0 is supported, with the exception of
				FLOWR loops and conditional (e.g. if/else)
				statements.

		separator	the string separator that should be used when
				the XPath returns multiple values but the code
				expects only one. The default values are as
				below, which can be modified or expanded in
				_global_config.py:

					"default"	(returns "; " – this will also
							be used if a given separator
							field is empty or unrecognised)
					"comma"
					"semi-colon"
					"space"
					"empty"		(concatenates without separator)

		format		the desired format of the data returned in
				the current column. The default values are as
				below. On the expectation that the input data
				may not always consistently meet the
				requirements of a given data format (not least
				the fact that Excel struggles with dates before
				1900), data values which fail to strictly
				adhere to the chosen data format will usually
				be silently formatted as text.

					"text"		(formats as string)
					"number"	(formats as double)
					"date"		(formats as date if before 1950,
							else date-like string)
					"boolean"	(true/false)
					"percentage"	(formats decimal as percentage,
							rounded to two decimal places)

		comment		an optional comment explaining the output of
				the XPath query, which (if present) will be
				included as a note attached to the relevant
				column heading in the .xlsx output file.

	All authority configuration files must begin with a column containing a
		unique identifier, in order for authority lookups to be possible.

	The default configuration files result in output files in .csv and .json
		format of the same name, as well as a combined .xlsx file with
		tabs corresponding to each configuration file.


3. Collection Files

	3.1. Source Files

	Any .xml file found in the collection directory or its sub-directories will
		be treated as a file containing data about an individual manuscript
		unit.

	3.2. Default Configuration Files

	Any .csv file found in the tabular_data/config/collection directory will
		be treated as configuration files for collection output files.
		These will result in output files in .csv and .json format of the
		same name, as well as a combined .xslx file with tabs corresponding
		to each configuration file.

	By default, all output files are configured to begin with columns containing
		comparable metadata such as shelfmark and the name of the collection
		file, to allow cross-comparison.

	The default configuration files are as follows:

		00_overview.csv				Each row of the output file corresponds to
							a manuscript unit, be that a part (`msPart`)
							or a whole MS (`msDesc`). This configuration
							file generates an output file that contains
							a variety of basic information about the
							unit in question, including details of its
							origin, provenance, acquisition, contents,
							codicology, palaeography, and binding, and
							a number of measurements.

		01_records.csv				Each row of the output file corresponds to
							an individual `additional` element within
							the collection data, and contains enhanced
							metadata about the object in question, as
							well as details of the record history, the
							availability of the object and any digital
							surrogates, and printed and digital
							resources associated with the object.

		02_binding.csv				Each row of the output file corresponds to
							an individual `binding` element or a
							`dimensions` element with `@type="binding"`.
							This contains overview information about
							the object's binding, as well as dating
							and measurement information.

		03_measurements.csv			Each row of the output file corresponds to
							an individual set of measurements for a
							manuscript unit, as encoded within either
							(or both) of `layout` and `extent` elements.
							This contains information about the
							manuscript part's extent, as well as
							measurements and other details associated
							with rolls, fragments, leaves, columns,
							intercolumn widths, the written area, and
							ruling.
							
		04_hands.csv				Each row of the output file corresponds to
							an individual `handNote` element for a
							manuscript unit. This contains overview
							information about the hand in question, as
							well as details of any people associated
							with the hand, any locus information,
							details of dating, and palaeographic
							details.

		05_contents.csv				Each row of the output file corresponds to
							an individual work or part of a work
							contained within a manuscript unit in the
							collection. This contains locus and length
							information, alongside details of the
							work's title subject, author, language,
							and paratext, as well as any bibliographic
							references associated with it in the
							collection file.

		06_music.csv				Each row of the output file corresponds to
							an individual description of musical
							notation encoded within a `musicNotation`
							element. This contains overview information
							about the notation in question, as well as
							locus and dimension information, and
							details of any people or bibliographic
							references associated with the notation.

		07_decoration.csv			Each row of the output file corresponds to
							an individual description of decoration
							encoded within a `decoNote` element. This
							contains overview information about the
							decoration in question, including its
							type, as well as locus and date information
							and details of any people, organisations,
							places, or bibliographic references
							associated with the decoration.

		08_origins.csv				Each row of the output file corresponds to
							an individual manuscript unit, and the
							information about its origins contained
							within the `origin` element (manuscript
							parts inherit data from their containing
							manuscript, should no other information be
							present). This contains overview
							information about the origin of the unit
							in question, as well as locus and date
							information, and details of any people,
							organisations, or bibliographic references
							associated with the manuscript unit's
							origins.

		09_additions.csv			Each row of the output file corresponds to
							an individual description of additions to
							a given manuscript unit, as given within
							the `additions` element. This contains
							overview information about the addition
							in question, as well as locus information,
							and details of any people or bibliographic
							references associated with the addition.

		10_provenance_acquisition.csv		Each row of the output file corresponds to
							an individual set of information given
							about the history of a manuscript unit, as
							contained within a `provenance` or
							`acquisition` element. This contains
							overview information about the historical
							detail in question, as well as date
							information, and details of any people,
							organisations, or places associated with
							the historical detail, with additional
							details given about former owners.

	3.3. Configuration File Columns

	Individual rows in the .csv configuration file will correspond to
		individual columns in the output file. In order for the
		data extraction to be successful, each .csv configuration
		file must have the following columns:

		section		a top-level heading to group different output
				columns together. Consecutive output columns
				with the same section heading will be grouped
				into a single section within the .xlsx output.
				This field is optional, but advised.

		heading		a heading for the output column, within the
				previously designated section.	

		xpath		the XPath query that should be used to extract
				the data for the present output column. Every
				XPath query in a given authority file must
				return exactly the same number of values as
				any other, else the code will fail. `string()`
				and `string-join()` are good ways of forcing
				multiple (or zero-length) values to be returned
				as a single value. XPath up to and including
				version 3.0 is supported, with the exception of
				FLOWR loops and conditional (e.g. if/else)
				statements. If you wish to perform an
				'authority lookup' and extract data from one
				of the authority output files, then this query
				must result in an identifier that corresponds
				with the identifier given in the first column
				of the relevant authority output file.

		auth_file	the name of the authority output file from
				which the data should be extracted, if you are
				performing an authority lookup. Else this can
				be left blank.

		auth_section	the top-level heading in the authority
				output file within which the desired lookup
				column can be found, if you are performing
				an authority lookup. This can be left blank
				if no authority lookup is being performed, or
				if the authority file has no section headings.

		auth_heading	the heading in the authority output file of
				the desired lookup column. This can be left
				blank if no authority lookup is being
				performed.

		separator	the string separator that should be used when
				an authority-lookup returns multiple values.
				The default values are as below, which can be
				modified or expanded in _global_config.py:

					"default"	(returns "; " – this will also
							be used if a given separator
							field is empty or unrecognised)
					"comma"
					"semi-colon"
					"space"
					"empty"		(concatenates without separator)

		format		the desired format of the data returned in
				the current column. The default values are as
				below. On the expectation that the input data
				may not always consistently meet the
				requirements of a given data format (not least
				the fact that Excel struggles with dates before
				1900), data values which fail to strictly
				adhere to the chosen data format will usually
				be silently formatted as text.

					"text"		(formats as string)
					"number"	(formats as double)
					"date"		(formats as date if before 1950,
							else date-like string, to avoid
							date-handling errors in Excel)
					"boolean"	(true/false)
					"percentage"	(formats decimal as percentage,
							rounded to two decimal places)

		comment		an optional comment explaining the output of
				the XPath query, which (if present) will be
				included as a note attached to the relevant
				column heading in the .xlsx output file.

