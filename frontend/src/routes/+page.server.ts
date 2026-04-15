import { type Actions } from '@sveltejs/kit';

export const actions = {
	default: async ({ request }) => {
		const data = await request.formData();
		const unames = data.getAll('user');
		const query = unames.map((uname) => `user=${uname}`).join('&');
		const response = await fetch(`http://127.0.0.1:8000/users?${query}`);
		const reader = response.body!.getReader();
		const decoder = new TextDecoder();

		let raw = '';
		while (true) {
			const { value, done } = await reader.read();
			if (done) break;
			raw += decoder.decode(value, { stream: true });
		}
		const dict: Record<string, Array<Record<string, string>>> = JSON.parse(raw);
		console.log(dict);
		return { success: true, userBooks: dict };
	}
} satisfies Actions;
