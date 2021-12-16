# Zooniverse
1. When removing subjects sets from a workflow
	* It takes some time for the statitics to reflect that (5 min)
	* The dataset export still includes the subjects in inactive subject set

2. When a subject set is deleted
	* It still takes count against your maximun limit of datasets (10000)
	* The dataset export still includes the subjects in the deleted subject set

3. When you click on "Next" even without using the tools, this counts as "classified".

4. When you see the "ALREADY SEEN!" banner this means that all subject sets has been doisplayed to a user
- For test of retirement count == 1 this means deleting the subject sets and creating a new one

2) Probar que pasa con manage_workflows cuando no hay subject sets
 ***RESPUESTA***
 Como detecte auqw no hay subjects, sale con True y procede a continuar con el workflow de recarga
3) probar a exportar sin subject sets
***RESPUESTA***
Exporta sin errores a un fichero con un JSON vacio []


# Valores enumerativos de la paleta de espectros
HPS - High Pressure Sodium. => 0
MV - Mercury Vapor. => 1
LED - Light Emitting Diode. => 2
MH - Metal Halide. => 3

