import { redirect, type Actions } from '@sveltejs/kit';

type Book = {
	title: string;
	storygraph_id: string;
	isbn: string;
	shelf?: string;
	rating?: number;
};

type BookList = Array<Book>;

type UserBooks = {
	status: string;
	userBooks: Record<string, BookList>;
	success?: boolean;
};

export const actions = {
	getBooks: async ({ request, cookies }) => {
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
		const dict: UserBooks = JSON.parse(raw);
		cookies.set('status', 'incomplete', { path: '/' });
		cookies.set('users', unames.join('&'), { path: '/' });
		dict['success'] = true;
		return dict;
	},

	getRecs: async ({ request }) => {
		// get group recommendations
		const data = await request.formData();
		const unames = data.getAll('user');
		const query = unames.map((uname) => `${uname}`).join('&');
		redirect(303, `/results/${query}`);
	}
} satisfies Actions;

export async function load({ cookies }) {
	if (!(cookies.get('users') && cookies.get('status'))) {
		return { success: false, status: 'N/A', userBooks: {} };
	} else {
		const users = cookies.get('users')!.split('&');
		const bookStatus = cookies.get('status');
		if (users) {
			// if (bookStatus !== 'complete') {
			const query = users.map((uname) => `user=${uname}`).join('&');
			const response = await fetch(`http://127.0.0.1:8000/users?${query}`);
			const reader = response.body!.getReader();
			const decoder = new TextDecoder();

			let raw = '';
			while (true) {
				const { value, done } = await reader.read();
				if (done) break;
				raw += decoder.decode(value, { stream: true });
			}
			const dict: UserBooks = JSON.parse(raw);
			cookies.set('status', dict.status, { path: '/' });
			dict['success'] = true;
			return dict;
		}
		// }
	}
}
// looking to call load function dependent on unames existing,
// and whether success=true
