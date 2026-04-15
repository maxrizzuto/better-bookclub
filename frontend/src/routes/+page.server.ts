import { redirect, type Actions } from '@sveltejs/kit';
import { userBooks } from './state.svelte';

export const actions = {
	default: async ({ request }) => {
		const data = await request.formData();
		const unames = data.getAll('user');
		const query = unames.map((uname) => `user=${uname}`).join('&');
		console.log(query);
		const response = await fetch(`http://127.0.0.1:8000/users?${query}`);
		const reader = response.body!.getReader();
		const decoder = new TextDecoder();

		let raw = '';
		while (true) {
			const { value, done } = await reader.read();
			if (done) break;
			raw += decoder.decode(value, { stream: true });
		}
		const dict = JSON.parse(raw);
		// redirect to loading page while data is fetched
		userBooks.books = dict;

		redirect(303, '/users');
	}
} satisfies Actions;
