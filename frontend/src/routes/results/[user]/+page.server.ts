import type { PageLoad, PageProps } from './$types';

type Book = {
	isbn: string;
	asin?: string;
	url: string;
	isbn13: string;
	image_url: string;
	book_id: number;
	work_id: number;
	title: string;
	ratings_count: number;
	preds: number;
};

type Recs = {
	usernames: Array<string>;
	group_results: Array<Book>;
	user_results: Record<string, Array<Book>>;
};

export const load: PageLoad = async ({ params }: { params: Record<'user', string> }) => {
	const users = params.user.split('&');
	const query = users.map((user) => `user=${user}`).join('&');
	const response = await fetch(`http://127.0.0.1:8000/recommendations?${query}`);
	const reader = response.body!.getReader();
	const decoder = new TextDecoder();

	let raw = '';
	while (true) {
		const { value, done } = await reader.read();
		if (done) break;
		raw += decoder.decode(value, { stream: true });
	}
	const dict: Recs = JSON.parse(raw);
	return dict;
};
