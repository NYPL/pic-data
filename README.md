# PIC Data Repository

By David Lowe, code by [Mauricio Giraldo](//twitter.com/mgiraldo), [NYPL Labs](//twitter.com/nypl_labs)

### View it at: [pic.nypl.org](http://pic.nypl.org)

The data itself is in the [`/csv` folder](csv/) and is described below. The [Python scripts](python/) are used to index the data in ElasticSearch and publish to Amazon S3.

See also the [PIC application repository](//github.com/nypl/pic-app).

## CSV files

These files are found in the [`/csv`](csv/) folder.

[address.csv](csv/address.csv) contains locations of Birth, Death and Activity, and street addresses for Studios. All have geographic coordinates. [addresstypes.csv](csv/addresstypes.csv) describes the addresses as either, Birth, Death, Active or Studio.

[biography.csv](csv/biography.csv) lists the source or sources from which an entry is derived. [biographies.csv](csv/biographies.csv) is a complete list of the sources.
[collection.csv](csv/collection.csv) lists which museums or libraries are known to have works by a photographer. [collections.csv](csv/collections.csv) is a complete list of the museums and libraries. More collections will be added periodically.

[constituents.csv](csv/constituents.csv) contains the Names, Nationalities, and Dates for over 115,000 photographers, studios and others in PIC. ConstituentType indicates whether the entry is for an Individual (1) or an Institution (2). Some have a biographical TextEntry, which will display in future iterations of PIC.

[countries.csv](csv/countries.csv) is the full list of countries found in [address.csv](csv/address.csv).

[format.csv](csv/format.csv) lists which types of photographs (such as Stereoscopic Photographs or Cabinet Cards) a photographer was known to have made. [formats.csv](csv/formats.csv) is a list of those formats currently tracked in PIC. Additional formats may be added in the future.

[gender.csv](csv/gender.csv) indicates whether a constituent is Female, Male, Other gender, or is a Studio (No Gender). [genders.csv](csv/genders.csv) lists those four options.

[nationality.csv](csv/nationality.csv) gives the Nationality for each constituent. Nationality is often presumed from the location(s) where the bulk of a photographer's or studio's work was produced, and does not necessarily reflect citizenship status. [nationalities.csv](csv/nationalities.csv) is the full list of Nationalities.

[process.csv](csv/process.csv) lists which photographic processes a constituent is known to have used (ie. Albumen Silver Prints or Gelatin Silver Prints). [processes.csv](csv/processes.csv) is the full list of processes currently tracked in PIC. Other processes will be added periodically.

[role.csv](csv/role.csv) indicates whether a Constituent was a Photographer, Manufacturer, Dealer of Photographic Supplies or other occupation within the discipline of photography. [roles.csv](csv/roles.csv) is the full list of Roles currently tracked in PIC. Other roles may be added periodically.

### External CSV files

[countries_wof.csv](csv/countries_wof.csv) was contributed by [Aaron Cope](https://github.com/thisisaaronland) and connects a `CountryID` in [countries.csv](csv/countries.csv) to Mapzen's [Who's On First data](https://github.com/whosonfirst/whosonfirst-data).

## License

See [LICENSE](LICENSE).
