package common

func DeduplicateStrings(input []string) []string {
	output := []string{}
	seen := make(map[string]struct{})
	for _, s := range input {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			output = append(output, s)
		}
	}
	return output
}
