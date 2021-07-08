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
LED - Light Emitting Diode. => 0
HPS - High Pressure Sodium. => 1
LPS - Low Pressure Sodium. => 2
MH - Metal Halide. => 3
MV - Mercury Vapor. => 4

# Sample run when classifying a subject set
[2021-07-08 19:22:49,249] {streetspectra.py:478} INFO - ######################## RATINGS = [{'subject_id': 63339761, 'source_id': 5, 'spectrum_type': 'Ambiguous', 'spectrum_dist': "[('HPS', 1), ('LED', 1)]", 'counter': Counter({'HPS': 1, 'LED': 1})}, {'subject_id': 63339761, 'source_id': 1, 'spectrum_type': 'LED', 'spectrum_dist': "[('LED', 2), ('HPS', 1)]", 'counter': Counter({'LED': 2, 'HPS': 1})}, {'subject_id': 63339761, 'source_id': 4, 'spectrum_type': 'LPS', 'spectrum_dist': "[('LPS', 1)]", 'counter': Counter({'LPS': 1})}]
p_j = {'LED': 0.16666666666666666, 'HPS': 0.1111111111111111, 'LPS': 0.05555555555555555, 'MV': 0.0, 'MH': 0.0, None: 0.0}
P_i = {1: -0.03333333333333333, 4: -0.16666666666666666, 5: -0.13333333333333333}
P_bar = -0.1111111111111111
P_e_bar = 0.043209876543209874
n_ij = {(5, 'LED'): 1, (5, 'HPS'): 1, (5, 'LPS'): 0, (5, 'MV'): 0, (5, 'MH'): 0, (5, None): 0, (1, 'LED'): 2, (1, 'HPS'): 1, (1, 'LPS'): 0, (1, 'MV'): 0, (1, 'MH'): 0, (1, None): 0, (4, 'LED'): 0, (4, 'HPS'): 0, (4, 'LPS'): 1, (4, 'MV'): 0, (4, 'MH'): 0, (4, None): 0}
[2021-07-08 19:22:49,250] {streetspectra.py:480} INFO - ######################## KAPPA = -0.16129032258064516, n = 6
