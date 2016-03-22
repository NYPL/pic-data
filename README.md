# PIC Data Repository

By David Lowe, code by [Mauricio Giraldo](//twitter.com/mgiraldo)

### View it at: [pic.nypl.org](http://pic.nypl.org)

The data itself is in the [`/csv` folder](csv/) and is described below. The [Python scripts](python/) are used to index the data in ElasticSearch and publish to Amazon S3.

See also the [PIC application repository](//github.com/nypl/pic-app).

### CSV files
Address.csv contains locations of Birth, Death and Activity, and street addresses for Studios. All have geographic coordinates. Addresstypes.csv describes the Addresses as either, Birth, Death, Active or Studio.

Biography.csv lists the source or sources from which an entry is derived. Biographies.csv is a complete list of the sources.
Collection.csv lists which museums or libraries are known to have works by a photographer. Collections.csv is a complete list of the museums and libraries. More collections will be added periodically.

Constituents.csv contains the Names, Nationalities, and Dates for over 115,000 photographers, studios and others in PIC. ConstituentType indicates whether the entry is for an Individual (1) or an Institution (2). Some have a biographical TextEntry, which will display in future iterations of PIC.

Countries.csv is the full list of countries found in Address.csv.

Format.csv lists which types of photographs (such as Stereoscopic Photographs or Cabinet Cards) a photographer was known to have made. Formats.csv is a list of those formats currently tracked in PIC. Additional formats may be added in the future.

Gender.csv indicates whether a constituent is Female, Male, Other gender, or is a Studio (No Gender). Genders.csv lists those four options.

Nationality.csv gives the Nationality for each constituent. Nationality is often presumed from the location(s) where the bulk of a photographer's or studio's work was produced, and does not necessarily reflect citizenship status. Nationalities.csv is the full list of Nationalities.

Process.csv lists which photographic processes a constituent is known to have used (ie. Albumen Silver Prints or Gelatin Silver Prints). Processes.csv is the full list of processes currently tracked in PIC. Other processes will be added periodically.

Role.csv indicates whether a Constituent was a Photographer, Manufacturer, Dealer of Photographic Supplies or other occupation within the discipline of photography. Roles.csv is the full list of Roles currently tracked in PIC. Other roles may be added periodically.


## License

See [LICENSE](LICENSE)
