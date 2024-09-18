--liquibase formatted sql
--changeset alp:V5.3.1.1.3__berlin_location

CREATE SEQUENCE berlin_zipcode_sequence
START WITH 1 INCREMENT BY 1;

INSERT INTO LOCATION
(location_id, zip, location_source_value) 

SELECT berlin_zipcode_sequence.nextval,* FROM 
(SELECT 10115 zip, 'Mitte' location_source_value FROM DUMMY UNION
SELECT 10117, 'Mitte' FROM DUMMY UNION
SELECT 10119, 'Mitte' FROM DUMMY UNION
SELECT 10119, 'Pankow' FROM DUMMY UNION
SELECT 10178, 'Mitte' FROM DUMMY UNION
SELECT 10179, 'Mitte' FROM DUMMY UNION
SELECT 10243, 'Friedrichshain-Kreuzberg' FROM DUMMY UNION
SELECT 10245, 'Friedrichshain-Kreuzberg' FROM DUMMY UNION
SELECT 10247, 'Friedrichshain-Kreuzberg' FROM DUMMY UNION
SELECT 10249, 'Friedrichshain-Kreuzberg' FROM DUMMY UNION
SELECT 10315, 'Lichtenberg' FROM DUMMY UNION
SELECT 10317, 'Lichtenberg' FROM DUMMY UNION
SELECT 10318, 'Lichtenberg' FROM DUMMY UNION
SELECT 10319, 'Lichtenberg' FROM DUMMY UNION
SELECT 10365, 'Lichtenberg' FROM DUMMY UNION
SELECT 10367, 'Lichtenberg' FROM DUMMY UNION
SELECT 10369, 'Lichtenberg' FROM DUMMY UNION
SELECT 10405, 'Pankow' FROM DUMMY UNION
SELECT 10407, 'Pankow' FROM DUMMY UNION
SELECT 10409, 'Pankow' FROM DUMMY UNION
SELECT 10435, 'Pankow' FROM DUMMY UNION
SELECT 10437, 'Pankow' FROM DUMMY UNION
SELECT 10439, 'Pankow' FROM DUMMY UNION
SELECT 10551, 'Mitte' FROM DUMMY UNION
SELECT 10553, 'Mitte' FROM DUMMY UNION
SELECT 10555, 'Mitte' FROM DUMMY UNION
SELECT 10557, 'Mitte' FROM DUMMY UNION
SELECT 10559, 'Mitte' FROM DUMMY UNION
SELECT 10585, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10587, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10589, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10623, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10625, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10627, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10629, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10707, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10709, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10711, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10713, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10715, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10717, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10719, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 10777, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 10779, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 10781, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 10783, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 10785, 'Mitte' FROM DUMMY UNION
SELECT 10787, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 10787, 'Mitte' FROM DUMMY UNION
SELECT 10789, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 10823, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 10825, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 10827, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 10829, 'Tempelhof-Schöneberg' FROM DUMMY UNION
/*SELECT 11011), */
SELECT 10961, 'Friedrichshain-Kreuzberg' FROM DUMMY UNION
SELECT 10963, 'Friedrichshain-Kreuzberg' FROM DUMMY UNION
SELECT 10965, 'Friedrichshain-Kreuzberg' FROM DUMMY UNION
SELECT 10967, 'Friedrichshain-Kreuzberg' FROM DUMMY UNION
SELECT 10969, 'Friedrichshain-Kreuzberg' FROM DUMMY UNION
SELECT 10997, 'Friedrichshain-Kreuzberg' FROM DUMMY UNION
SELECT 10999, 'Friedrichshain-Kreuzberg' FROM DUMMY UNION
SELECT 12043, 'Neukölln' FROM DUMMY UNION
SELECT 12045, 'Neukölln' FROM DUMMY UNION
SELECT 12047, 'Neukölln' FROM DUMMY UNION
SELECT 12049, 'Neukölln' FROM DUMMY UNION
SELECT 12051, 'Neukölln' FROM DUMMY UNION
SELECT 12053, 'Neukölln' FROM DUMMY UNION
SELECT 12055, 'Neukölln' FROM DUMMY UNION
SELECT 12057, 'Neukölln' FROM DUMMY UNION
SELECT 12059, 'Neukölln' FROM DUMMY UNION
SELECT 12099, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12101, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12103, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12105, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12107, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12109, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12157, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12157, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 12159, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12161, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12161, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 12163, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 12165, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 12167, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 12169, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 12169, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12203, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 12205, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 12207, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 12209, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 12247, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 12249, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 12277, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12279, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12305, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12307, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12309, 'Tempelhof-Schöneberg' FROM DUMMY UNION
SELECT 12347, 'Neukölln' FROM DUMMY UNION
SELECT 12349, 'Neukölln' FROM DUMMY UNION
SELECT 12351, 'Neukölln' FROM DUMMY UNION
SELECT 12353, 'Neukölln' FROM DUMMY UNION
SELECT 12355, 'Neukölln' FROM DUMMY UNION
SELECT 12357, 'Neukölln' FROM DUMMY UNION
SELECT 12359, 'Neukölln' FROM DUMMY UNION
SELECT 12435, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12437, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12439, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12459, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12487, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12489, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12524, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12526, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12527, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12529, 'Schönefeld bei Berlin' FROM DUMMY UNION
SELECT 12555, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12555, 'Marzahn-Hellersdorf' FROM DUMMY UNION
SELECT 12557, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12559, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12587, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12589, 'Treptow-Köpenick' FROM DUMMY UNION
SELECT 12619, 'Marzahn-Hellersdorf' FROM DUMMY UNION
SELECT 12621, 'Marzahn-Hellersdorf' FROM DUMMY UNION
SELECT 12623, 'Marzahn-Hellersdorf' FROM DUMMY UNION
SELECT 12627, 'Marzahn-Hellersdorf' FROM DUMMY UNION
SELECT 12629, 'Marzahn-Hellersdorf' FROM DUMMY UNION
SELECT 12679, 'Marzahn-Hellersdorf' FROM DUMMY UNION
SELECT 12681, 'Marzahn-Hellersdorf' FROM DUMMY UNION
SELECT 12683, 'Marzahn-Hellersdorf' FROM DUMMY UNION
SELECT 12685, 'Marzahn-Hellersdorf' FROM DUMMY UNION
SELECT 12687, 'Marzahn-Hellersdorf' FROM DUMMY UNION
SELECT 12689, 'Marzahn-Hellersdorf' FROM DUMMY UNION
SELECT 13051, 'Lichtenberg' FROM DUMMY UNION
SELECT 13051, 'Pankow' FROM DUMMY UNION
SELECT 13053, 'Lichtenberg' FROM DUMMY UNION
SELECT 13055, 'Lichtenberg' FROM DUMMY UNION
SELECT 13057, 'Lichtenberg' FROM DUMMY UNION
SELECT 13059, 'Lichtenberg' FROM DUMMY UNION
SELECT 13086, 'Pankow' FROM DUMMY UNION
SELECT 13088, 'Pankow' FROM DUMMY UNION
SELECT 13089, 'Pankow' FROM DUMMY UNION
SELECT 13125, 'Pankow' FROM DUMMY UNION
SELECT 13127, 'Pankow' FROM DUMMY UNION
SELECT 13129, 'Pankow' FROM DUMMY UNION
SELECT 13156, 'Pankow' FROM DUMMY UNION
SELECT 13158, 'Pankow' FROM DUMMY UNION
SELECT 13159, 'Pankow' FROM DUMMY UNION
SELECT 13187, 'Pankow' FROM DUMMY UNION
SELECT 13189, 'Pankow' FROM DUMMY UNION
SELECT 13347, 'Mitte' FROM DUMMY UNION
SELECT 13349, 'Mitte' FROM DUMMY UNION
SELECT 13351, 'Mitte' FROM DUMMY UNION
SELECT 13353, 'Mitte' FROM DUMMY UNION
SELECT 13355, 'Mitte' FROM DUMMY UNION
SELECT 13357, 'Mitte' FROM DUMMY UNION
SELECT 13359, 'Mitte' FROM DUMMY UNION
SELECT 13403, 'Reinickendorf' FROM DUMMY UNION
SELECT 13405, 'Reinickendorf' FROM DUMMY UNION
SELECT 13407, 'Reinickendorf' FROM DUMMY UNION
SELECT 13407, 'Mitte' FROM DUMMY UNION
SELECT 13409, 'Reinickendorf' FROM DUMMY UNION
SELECT 13409, 'Mitte' FROM DUMMY UNION
SELECT 13435, 'Reinickendorf' FROM DUMMY UNION
SELECT 13437, 'Reinickendorf' FROM DUMMY UNION
SELECT 13439, 'Reinickendorf' FROM DUMMY UNION
SELECT 13465, 'Reinickendorf' FROM DUMMY UNION
SELECT 13467, 'Reinickendorf' FROM DUMMY UNION
SELECT 13469, 'Reinickendorf' FROM DUMMY UNION
SELECT 13503, 'Reinickendorf' FROM DUMMY UNION
SELECT 13505, 'Reinickendorf' FROM DUMMY UNION
SELECT 13507, 'Reinickendorf' FROM DUMMY UNION
SELECT 13509, 'Reinickendorf' FROM DUMMY UNION
SELECT 13581, 'Spandau' FROM DUMMY UNION
SELECT 13583, 'Spandau' FROM DUMMY UNION
SELECT 13585, 'Spandau' FROM DUMMY UNION
SELECT 13587, 'Spandau' FROM DUMMY UNION
SELECT 13589, 'Spandau' FROM DUMMY UNION
SELECT 13591, 'Spandau' FROM DUMMY UNION
SELECT 13593, 'Spandau' FROM DUMMY UNION
SELECT 13595, 'Spandau' FROM DUMMY UNION
SELECT 13597, 'Spandau' FROM DUMMY UNION
SELECT 13599, 'Spandau' FROM DUMMY UNION
SELECT 13627, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 13629, 'Spandau' FROM DUMMY UNION
SELECT 14050, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 14052, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 14053, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 14055, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 14057, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 14059, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 14089, 'Spandau' FROM DUMMY UNION
SELECT 14109, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 14129, 'Steglitz-Zehlendorf' FROM DUMMY UNION
/*SELECT 14131), */
SELECT 14163, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 14165, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 14167, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 14169, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 14193, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 14193, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 14195, 'Steglitz-Zehlendorf' FROM DUMMY UNION
SELECT 14195, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 14197, 'Charlottenburg-Wilmersdorf' FROM DUMMY UNION
SELECT 14199, 'Charlottenburg-Wilmersdorf' FROM DUMMY)